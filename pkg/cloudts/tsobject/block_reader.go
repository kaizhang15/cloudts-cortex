package tsobject

import (
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
)

// 获取单个Block内指定seriesRef的chunks
func GetSeriesChunks(blockDir string, seriesRef uint32) ([]chunks.Meta, error) {
	// 1. 打开Block
	block, err := tsdb.OpenBlock(nil, blockDir, nil)
	if err != nil {
		return nil, err
	}
	defer block.Close()

	// 2. 获取索引读取器
	indexr, err := block.Index()
	if err != nil {
		return nil, err
	}
	defer indexr.Close()

	// 3. 获取Chunk读取器
	chunkr, err := block.Chunks()
	if err != nil {
		return nil, err
	}
	defer chunkr.Close()

	// 4. 通过seriesRef查询对应的Chunk引用
	chunkRefs := []chunks.ChunkRef{}
	if err := indexr.Series(seriesRef, nil, &chunkRefs); err != nil {
		return nil, err
	}

	// 5. 加载实际的Chunk数据
	result := make([]chunks.Meta, 0, len(chunkRefs))
	for _, ref := range chunkRefs {
		chk, err := chunkr.Chunk(ref)
		if err != nil {
			return nil, err
		}
		result = append(result, chunks.Meta{
			Chunk:   chk,
			MinTime: chk.MinTime(),
			MaxTime: chk.MaxTime(),
		})
	}

	return result, nil
}

// 示例：构建完整的 seriesChunks 映射
func BuildSeriesChunksMap(blockDir string, targetSeries []uint32) (map[uint32][]chunks.Meta, error) {
	seriesChunks := make(map[uint32][]chunks.Meta)

	for _, ref := range targetSeries {
		chunks, err := GetSeriesChunks(blockDir, ref)
		if err != nil {
			return nil, err
		}
		seriesChunks[ref] = chunks
	}

	return seriesChunks, nil
}


func ProcessFullBlock(blockDir string) error {
	// 1. 获取Block内所有时序的ID
	block, err := tsdb.OpenBlock(nil, blockDir, nil)
	if err != nil {
		return err
	}
	defer block.Close()

	indexr, err := block.Index()
	if err != nil {
		return err
	}
	defer indexr.Close()

	// 获取所有时序ID
	allSeries := []uint32{}
	postings, err := indexr.Postings("", "") // 空匹配获取全部
	if err != nil {
		return err
	}
	for postings.Next() {
		allSeries = append(allSeries, postings.At())
	}

	// 2. 构建seriesChunks
	seriesChunks, err := BuildSeriesChunksMap(blockDir, allSeries)
	if err != nil {
		return err
	}

	// 3. 构建TSObject（按标签分组）
	grouped := GroupSeriesByLabelSimilarity(seriesChunks) // 需实现分组逻辑
	for groupID, chunks := range grouped {
		obj, err := cloudts.BuildTSObject(groupID, chunks)
		if err != nil {
			return err
		}
		// 上传obj到S3...
	}
	return nil
}


func ProcessFilteredSeries(blockDir string, labelName, labelValue string) error {
	// 1. 通过标签过滤时序
	block, err := tsdb.OpenBlock(nil, blockDir, nil)
	if err != nil {
		return err
	}
	defer block.Close()

	indexr, err := block.Index()
	if err != nil {
		return err
	}
	defer indexr.Close()

	// 获取匹配标签的时序ID
	filteredSeries := []uint32{}
	postings, err := indexr.Postings(labelName, labelValue)
	if err != nil {
		return err
	}
	for postings.Next() {
		filteredSeries = append(filteredSeries, postings.At())
	}

	// 2. 构建seriesChunks
	seriesChunks, err := BuildSeriesChunksMap(blockDir, filteredSeries)
	if err != nil {
		return err
	}

	// 3. 构建TSObject（按标签分组）
	grouped := GroupSeriesByLabelSimilarity(seriesChunks) // 需实现分组逻辑
	for groupID, chunks := range grouped {
		obj, err := cloudts.BuildTSObject(groupID, chunks)
		if err != nil {
			return err
		}
		// 上传obj到S3...
	}
	return nil
}