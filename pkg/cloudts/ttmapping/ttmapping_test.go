package ttmapping

import (
	"os"
	"testing"
	"time"
	
	"github.com/stretchr/testify/assert"
)

func TestProtoRoundTrip(t *testing.T) {
	dense := [][]bool{
		{true, false, true},
		{false, true, false},
		{true, true, true},
	}
	seriesIDs := []uint64{1, 2, 3}
	original := NewFromDense(dense, seriesIDs)
	original.partition = 123
	original.lastUpdated = time.Now().Truncate(time.Nanosecond)

	protoData := original.ToProto()
	restored := NewFromProto(protoData)

	assert.Equal(t, original.partition, restored.partition)
	assert.Equal(t, original.timeseries, restored.timeseries)
	assert.Equal(t, original.bitmap.Indices, restored.bitmap.Indices)
	assert.Equal(t, original.bitmap.Pointers, restored.bitmap.Pointers)
	assert.Equal(t, original.bitmap.Rows, restored.bitmap.Rows)
	assert.Equal(t, original.bitmap.Cols, restored.bitmap.Cols)
	assert.Equal(t, original.lastUpdated, restored.lastUpdated)
}

func TestFilePersistence(t *testing.T) {
	dense := [][]bool{
		{true, false},
		{false, true},
	}
	seriesIDs := []uint64{101, 102}
	mapping := NewFromDense(dense, seriesIDs)
	mapping.partition = 456
	mapping.lastUpdated = time.Now()

	tmpFile := "/tmp/ttmapping_test.bin"
	defer os.Remove(tmpFile)

	err := mapping.SaveToFile(tmpFile)
	assert.NoError(t, err)

	loaded, err := LoadFromFile(tmpFile)
	assert.NoError(t, err)

	assert.Equal(t, []uint64{101}, loaded.GetSeriesByTag(0))
	assert.Equal(t, []uint64{102}, loaded.GetSeriesByTag(1))
	assert.Equal(t, []uint32{0}, loaded.GetTagsBySeries(101))
	assert.Equal(t, []uint32{1}, loaded.GetTagsBySeries(102))
}