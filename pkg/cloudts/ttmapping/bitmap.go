package ttmapping

type TTMapping struct {
    TimePartition uint64
    TimeseriesIDs []uint64
    SparseMatrix  *CompressedBitmap
}

type CompressedBitmap struct {
    Indices  []uint32 // 非零位的列索引（按行展开）
    Pointers []int    // 每行的起始位置（类似 CSR 格式）
}

// 压缩位图（输入为二维 bool 矩阵）
func (cb *CompressedBitmap) Compress(bitmap [][]bool) {
    cb.Pointers = append(cb.Pointers, 0)
    for _, row := range bitmap {
        for j, val := range row {
            if val {
                cb.Indices = append(cb.Indices, uint32(j))
            }
        }
        cb.Pointers = append(cb.Pointers, len(cb.Indices))
    }
}

// 根据标签编码查询时间序列 ID
func (ttm *TTMapping) GetTimeseriesByTag(tagEncoding uint32) []uint64 {
    var result []uint64
    for i := 0; i < len(ttm.SparseMatrix.Pointers)-1; i++ {
        start := ttm.SparseMatrix.Pointers[i]
        end := ttm.SparseMatrix.Pointers[i+1]
        for _, col := range ttm.SparseMatrix.Indices[start:end] {
            if col == tagEncoding {
                result = append(result, ttm.TimeseriesIDs[i])
                break
            }
        }
    }
    return result
}