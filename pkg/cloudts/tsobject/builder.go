package tsobject

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/prometheus/prometheus/tsdb/chunks"
)

var bufPool = sync.Pool{
	New: func() interface{} { return bytes.NewBuffer(make([]byte, 0, 64<<20)) },
}

type TSObjectBuilder struct {
	groupID   uint32
	buf       *bytes.Buffer
	chunkRefs []ChunkRef
	minTime   int64
	maxTime   int64
}

func NewTSObjectBuilder(groupID uint32) *TSObjectBuilder {
	return &TSObjectBuilder{
		groupID: groupID,
		buf:     bufPool.Get().(*bytes.Buffer),
	}
}

func (b *TSObjectBuilder) AddChunk(seriesRef uint32, chunk chunks.Meta) error {
	if len(b.chunkRefs) >= MaxSeriesPerObj {
		return errors.New("max series per object exceeded")
	}

	chunkData := chunk.Chunk.Bytes()
	offset := uint32(b.buf.Len())

	// 更新时间范围
	if len(b.chunkRefs) == 0 {
		b.minTime, b.maxTime = chunk.MinTime, chunk.MaxTime
	} else {
		if chunk.MinTime < b.minTime {
			b.minTime = chunk.MinTime
		}
		if chunk.MaxTime > b.maxTime {
			b.maxTime = chunk.MaxTime
		}
	}

	// 记录Chunk引用
	b.chunkRefs = append(b.chunkRefs, ChunkRef{
		SeriesRef:  seriesRef,
		MinTime:    chunk.MinTime,
		MaxTime:    chunk.MaxTime,
		DataOffset: offset,
		DataLength: uint32(len(chunkData)),
		Checksum:   crc32.ChecksumIEEE(chunkData),
		Encoding:   chunk.Chunk.Encoding(),
	})

	// 写入数据
	_, err := b.buf.Write(chunkData)
	return err
}

func (b *TSObjectBuilder) Build() (*TSObject, error) {
	if len(b.chunkRefs) == 0 {
		return nil, errors.New("no chunks added")
	}

	header := Header{
		Magic:       MagicNumber,
		Version:     VersionV1,
		GroupID:     b.groupID,
		MinTime:     b.minTime,
		MaxTime:     b.maxTime,
		SeriesCount: uint16(len(b.chunkRefs)),
		ChunkCount:  uint32(len(b.chunkRefs)),
		ChunkRefs:   b.chunkRefs,
	}

	// 验证头部大小
	if headerSize := binary.Size(header); headerSize > HeaderSize {
		return nil, fmt.Errorf("header size %d exceeds limit %d", headerSize, HeaderSize)
	}

	return &TSObject{
		Header:  header,
		RawData: b.buf.Bytes(),
	}, nil
}

func (b *TSObjectBuilder) Reset() {
	b.buf.Reset()
	b.chunkRefs = b.chunkRefs[:0]
	b.minTime, b.maxTime = 0, 0
}
