package tsobject

import (
	"errors"
	"hash/crc32"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type ChunkReader struct {
	obj *TSObject
}

func NewChunkReader(obj *TSObject) *ChunkReader {
	return &ChunkReader{obj: obj}
}

func (r *ChunkReader) Chunk(ref ChunkRef) (chunkenc.Chunk, error) {
	if int(ref.DataOffset+ref.DataLength) > len(r.obj.RawData) {
		return nil, errors.New("chunk data out of range")
	}

	data := r.obj.RawData[ref.DataOffset : ref.DataOffset+ref.DataLength]
	if crc32.ChecksumIEEE(data) != ref.Checksum {
		return nil, errors.New("invalid chunk checksum")
	}

	return chunkenc.FromData(ref.Encoding, data)
}

func (r *ChunkReader) Series(ref uint32) ([]ChunkRef, error) {
	var result []ChunkRef
	for _, cr := range r.obj.Header.ChunkRefs {
		if cr.SeriesRef == ref {
			result = append(result, cr)
		}
	}
	return result, nil
}

func (r *ChunkReader) TimeRange() (minTime, maxTime int64) {
	return r.obj.Header.MinTime, r.obj.Header.MaxTime
}
