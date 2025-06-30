package tsobject

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sync"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

// TSObject 头部固定结构（总长度 4KB）
const (
	headerSize      = 4096
	magicNumber     = 0xC1A550DB // 魔数标识文件格式
	currentVersion  = 1
	maxSeriesPerObj = 65535      // 单个TSObject最大时序数
)

var (
	// 全局缓冲池（减少GC压力）
	bufPool = sync.Pool{
		New: func() interface{} { return bytes.NewBuffer(make([]byte, 0, 16<<20)) },
	}
)

// TSObject 结构定义
type TSObject struct {
	Header
	RawData []byte // 原始Chunk数据（保持Cortex压缩格式）
}

// Header 二进制布局（小端序）
type Header struct {
	Magic        uint32
	Version      uint16
	GroupID      uint32    // 时序分组ID
	MinTime      int64     // 最小时间戳（纳秒）
	MaxTime      int64     // 最大时间戳（纳秒）
	SeriesCount  uint16    // 当前对象包含的时序数
	_            [6]byte   // 对齐填充
	ChunkRefs    []ChunkRef // 动态部分（实际存储时紧接在固定头部之后）
}

// ChunkRef 块引用信息
type ChunkRef struct {
	SeriesRef  uint32     // 时序ID（对应TTMapping中的行号）
	MinTime    int64      // 块最小时间（冗余存储加速查询）
	MaxTime    int64      // 块最大时间
	DataOffset uint32     // 在RawData中的偏移量
	DataLength uint32     // 块数据长度
	Checksum   uint32     // CRC32校验和
}

// BuildTSObject 从Cortex Chunks构建TSObject
func BuildTSObject(groupID uint32, seriesChunks map[uint32][]chunks.Meta) (*TSObject, error) {
	if len(seriesChunks) > maxSeriesPerObj {
		return nil, errors.New("too many series in one TSObject")
	}

	// 从缓冲池获取内存
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()

	obj := &TSObject{
		Header: Header{
			Magic:       magicNumber,
			Version:     currentVersion,
			GroupID:     groupID,
			SeriesCount: uint16(len(seriesChunks)),
		},
	}

	// 1. 收集所有Chunk并计算时间范围
	var firstChunk = true
	for seriesRef, chunkList := range seriesChunks {
		for _, chunk := range chunkList {
			// 更新时间范围
			if firstChunk {
				obj.Header.MinTime = chunk.MinTime
				obj.Header.MaxTime = chunk.MaxTime
				firstChunk = false
			} else {
				if chunk.MinTime < obj.Header.MinTime {
					obj.Header.MinTime = chunk.MinTime
				}
				if chunk.MaxTime > obj.Header.MaxTime {
					obj.Header.MaxTime = chunk.MaxTime
				}
			}

			// 序列化Chunk数据
			chunkData := chunk.Chunk.Bytes()
			startOffset := uint32(buf.Len())

			if _, err := buf.Write(chunkData); err != nil {
				return nil, err
			}

			// 记录Chunk引用
			obj.Header.ChunkRefs = append(obj.Header.ChunkRefs, ChunkRef{
				SeriesRef:  seriesRef,
				MinTime:    chunk.MinTime,
				MaxTime:    chunk.MaxTime,
				DataOffset: startOffset,
				DataLength: uint32(len(chunkData)),
				Checksum:   crc32.ChecksumIEEE(chunkData),
			})
		}
	}

	// 2. 合并数据并验证头部大小
	obj.RawData = make([]byte, buf.Len())
	copy(obj.RawData, buf.Bytes())

	if err := obj.validateHeaderSize(); err != nil {
		return nil, err
	}

	return obj, nil
}

// validateHeaderSize 确保头部不超过4KB限制
func (o *TSObject) validateHeaderSize() error {
	// 计算动态部分大小
	refsSize := len(o.Header.ChunkRefs) * binary.Size(ChunkRef{})
	totalSize := binary.Size(o.Header) - binary.Size(o.Header.ChunkRefs) + refsSize

	if totalSize > headerSize {
		return errors.New("TSObject header exceeds 4KB limit")
	}
	return nil
}

// WriteTo 实现io.WriterTo接口（用于上传到S3）
func (o *TSObject) WriteTo(w io.Writer) (n int64, err error) {
	// 1. 写入固定头部
	if err := binary.Write(w, binary.LittleEndian, o.Header.Magic); err != nil {
		return n, err
	}
	n += 4

	// 2. 写入动态部分（ChunkRefs）
	for _, ref := range o.Header.ChunkRefs {
		if err := binary.Write(w, binary.LittleEndian, ref); err != nil {
			return n, err
		}
		n += int64(binary.Size(ref))
	}

	// 3. 写入原始数据
	dataWritten, err := w.Write(o.RawData)
	return n + int64(dataWritten), err
}

// ReadFrom 实现io.ReaderFrom接口（从S3加载）
func (o *TSObject) ReadFrom(r io.Reader) (n int64, err error) {
	// 实现略（需处理头部解析和数据加载）
	// ...
	return n, nil
}

// GetChunk 按引用读取单个Chunk
func (o *TSObject) GetChunk(ref ChunkRef) (chunkenc.Chunk, error) {
	// 边界检查
	if int(ref.DataOffset+ref.DataLength) > len(o.RawData) {
		return nil, errors.New("chunk data out of range")
	}

	// 校验数据
	data := o.RawData[ref.DataOffset : ref.DataOffset+ref.DataLength]
	if crc32.ChecksumIEEE(data) != ref.Checksum {
		return nil, errors.New("chunk checksum mismatch")
	}

	// 重建Prometheus Chunk（假设使用默认的XOR压缩）
	return chunkenc.FromData(chunkenc.EncXOR, data)
}