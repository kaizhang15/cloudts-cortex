package tsobject

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"time"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

const (
	MagicNumber       = 0xC1A550DB
	VersionV1         = 1
	HeaderSize        = 4096
	MaxSeriesPerObj   = 65535
	DefaultS3ACL      = "bucket-owner-full-control"
	S3UploadTimeout   = 5 * time.Minute
	S3DownloadTimeout = 3 * time.Minute
)

type S3Config struct {
	BucketName       string
	Prefix           string
	Region           string
	Endpoint         string // 可选
	S3ForcePathStyle bool
}

type Header struct {
	Magic       uint32
	Version     uint16
	GroupID     uint32
	MinTime     int64
	MaxTime     int64
	SeriesCount uint16
	ChunkCount  uint32
	_           [2]byte // Padding
	ChunkRefs   []ChunkRef
}

type ChunkRef struct {
	SeriesRef  uint32
	MinTime    int64
	MaxTime    int64
	DataOffset uint32
	DataLength uint32
	Checksum   uint32
	Encoding   chunkenc.Encoding
}

type TSObject struct {
	Header
	RawData []byte
}

var (
	ErrInvalidMagic       = errors.New("invalid magic number")
	ErrUnsupportedVersion = errors.New("unsupported version")
	ErrHeaderTooLarge     = errors.New("header size exceeds limit")
	ErrMaxSeriesExceeded  = errors.New("max series per object exceeded")
	ErrChunkOutOfRange    = errors.New("chunk data out of range")
	ErrChecksumMismatch   = errors.New("chunk checksum mismatch")
)

func decodeHeader(r io.Reader) (*Header, error) {
	var h Header
	if err := binary.Read(r, binary.LittleEndian, &h.Magic); err != nil {
		return nil, err
	}
	if h.Magic != MagicNumber {
		return nil, errors.New("invalid magic number")
	}

	if err := binary.Read(r, binary.LittleEndian, &h.Version); err != nil {
		return nil, err
	}
	if h.Version != VersionV1 {
		return nil, errors.New("unsupported version")
	}

	// 读取固定部分
	fixedPart := struct {
		GroupID     uint32
		MinTime     int64
		MaxTime     int64
		SeriesCount uint16
		ChunkCount  uint32
	}{}
	if err := binary.Read(r, binary.LittleEndian, &fixedPart); err != nil {
		return nil, err
	}

	h.GroupID = fixedPart.GroupID
	h.MinTime = fixedPart.MinTime
	h.MaxTime = fixedPart.MaxTime
	h.SeriesCount = fixedPart.SeriesCount
	h.ChunkCount = fixedPart.ChunkCount

	// 读取ChunkRefs
	h.ChunkRefs = make([]ChunkRef, h.ChunkCount)
	for i := uint32(0); i < h.ChunkCount; i++ {
		if err := binary.Read(r, binary.LittleEndian, &h.ChunkRefs[i]); err != nil {
			return nil, err
		}
	}

	return &h, nil
}

// Serialize 将TSObject序列化为字节流
func (o *TSObject) Serialize() ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	// 1. 写入头部
	if _, err := o.WriteTo(buf); err != nil {
		return nil, err
	}

	// 2. 写入原始数据
	if _, err := buf.Write(o.RawData); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Deserialize 从字节流反序列化TSObject
func Deserialize(data []byte) (*TSObject, error) {
	if len(data) < HeaderSize {
		return nil, errors.New("data too short for header")
	}

	// 1. 解析头部
	header, err := decodeHeader(bytes.NewReader(data[:HeaderSize]))
	if err != nil {
		return nil, err
	}

	// 2. 验证数据长度
	expectedLen := HeaderSize +
		len(header.ChunkRefs)*binary.Size(ChunkRef{}) +
		int(header.ChunkRefs[len(header.ChunkRefs)-1].DataOffset) +
		int(header.ChunkRefs[len(header.ChunkRefs)-1].DataLength)
	if len(data) < expectedLen {
		return nil, errors.New("data length mismatch")
	}

	// 3. 提取原始数据
	rawData := data[HeaderSize:]

	return &TSObject{
		Header:  *header,
		RawData: rawData,
	}, nil
}

// WriteTo 只写header
func (o *TSObject) WriteTo(w io.Writer) (int64, error) {
	var written int64

	// 写入固定头部
	if err := binary.Write(w, binary.LittleEndian, o.Header.Magic); err != nil {
		return written, err
	}
	written += 4

	if err := binary.Write(w, binary.LittleEndian, o.Header.Version); err != nil {
		return written, err
	}
	written += 2

	// 写入固定部分
	fixedPart := struct {
		GroupID     uint32
		MinTime     int64
		MaxTime     int64
		SeriesCount uint16
		ChunkCount  uint32
		_           [2]byte
	}{
		GroupID:     o.Header.GroupID,
		MinTime:     o.Header.MinTime,
		MaxTime:     o.Header.MaxTime,
		SeriesCount: o.Header.SeriesCount,
		ChunkCount:  uint32(len(o.Header.ChunkRefs)),
	}
	if err := binary.Write(w, binary.LittleEndian, fixedPart); err != nil {
		return written, err
	}
	written += int64(binary.Size(fixedPart))

	// 写入ChunkRefs
	for _, ref := range o.Header.ChunkRefs {
		if err := binary.Write(w, binary.LittleEndian, ref); err != nil {
			return written, err
		}
		written += int64(binary.Size(ref))
	}

	// 填充头部到HeaderSize
	if written < HeaderSize {
		padding := make([]byte, HeaderSize-written)
		n, err := w.Write(padding)
		return written + int64(n), err
	}

	return written, nil
}

func (o *TSObject) WriteToAll(w io.Writer) (int64, error) {
	var written int64

	// 写入固定头部
	if err := binary.Write(w, binary.LittleEndian, o.Header.Magic); err != nil {
		return written, err
	}
	written += 4

	if err := binary.Write(w, binary.LittleEndian, o.Header.Version); err != nil {
		return written, err
	}
	written += 2

	// 写入固定部分
	fixedPart := struct {
		GroupID     uint32
		MinTime     int64
		MaxTime     int64
		SeriesCount uint16
		ChunkCount  uint32
		_           [2]byte
	}{
		GroupID:     o.Header.GroupID,
		MinTime:     o.Header.MinTime,
		MaxTime:     o.Header.MaxTime,
		SeriesCount: o.Header.SeriesCount,
		ChunkCount:  uint32(len(o.Header.ChunkRefs)),
	}
	if err := binary.Write(w, binary.LittleEndian, fixedPart); err != nil {
		return written, err
	}
	written += int64(binary.Size(fixedPart))

	// 写入ChunkRefs
	for _, ref := range o.Header.ChunkRefs {
		if err := binary.Write(w, binary.LittleEndian, ref); err != nil {
			return written, err
		}
		written += int64(binary.Size(ref))
	}

	// 写入原始数据
	n, err := w.Write(o.RawData)
	return written + int64(n), err
}
