// pkg/cloudts/cloudts_manager.go
package cloudts

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tagarray"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tagdict"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tsobject"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/ttmapping"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

// CloudTSManager 核心管理结构
type CloudTSManager struct {
	logger        log.Logger
	s3Config      S3Config
	globalTagDict *tagdict.TagDict
	cloudWriter   *CloudWriter
	cloudQuerier  *CloudQuerier

	// 分区管理
	partitionMutex sync.RWMutex
	partitions     map[uint64]*timePartition // key: partition timestamp
	closeChan      chan struct{}
	wg             sync.WaitGroup
}

type timePartition struct {
	id        uint64
	tagArray  *tagarray.TagArray
	ttMapping *ttmapping.TTMapping
	tsObjects map[uint32]*tsobject.TSObject // key: groupID
	lastUsed  time.Time
	startTime time.Time
	Duration  uint16 // default: 2
	mu        sync.Mutex
}

// NewCloudTSManager 创建管理器实例
func NewCloudTSManager(s3Config S3Config, logger log.Logger) *CloudTSManager {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	mgr := &CloudTSManager{
		logger:        logger,
		s3Config:      s3Config,
		globalTagDict: tagdict.NewTagDict(),
		partitions:    make(map[uint64]*timePartition),
		closeChan:     make(chan struct{}),
	}

	// 初始化云组件
	mgr.cloudWriter = newCloudWriter(s3Config, logger)
	mgr.cloudQuerier = newCloudQuerier(s3Config, logger)

	// 启动后台清理协程
	mgr.wg.Add(1)
	go mgr.janitorLoop()

	return mgr
}

// Close 清理资源
func (m *CloudTSManager) Close() error {
	close(m.closeChan)
	m.wg.Wait()

	// 关闭所有子组件
	if err := m.cloudQuerier.Close(); err != nil {
		level.Error(m.logger).Log("msg", "failed to close querier", "err", err)
	}
	return nil
}

// janitorLoop 后台清理协程
func (m *CloudTSManager) janitorLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanupInactivePartitions()
		case <-m.closeChan:
			return
		}
	}
}

// cleanupInactivePartitions 清理不活跃分区
func (m *CloudTSManager) cleanupInactivePartitions() {
	m.partitionMutex.Lock()
	defer m.partitionMutex.Unlock()

	threshold := time.Now().Add(-2 * time.Hour) // 2小时不活跃则清理
	for id, part := range m.partitions {
		part.mu.Lock()
		if part.lastUsed.Before(threshold) {
			delete(m.partitions, id)
			level.Debug(m.logger).Log("msg", "cleaned inactive partition", "partition", id)
		}
		part.mu.Unlock()
	}
}

// getOrCreatePartition 获取或创建分区
func (m *CloudTSManager) getOrCreatePartition(partitionID uint64) *timePartition {
	m.partitionMutex.RLock()
	if p, exists := m.partitions[partitionID]; exists {
		m.partitionMutex.RUnlock()
		p.mu.Lock()
		p.lastUsed = time.Now()
		p.mu.Unlock()
		return p
	}
	m.partitionMutex.RUnlock()

	m.partitionMutex.Lock()
	defer m.partitionMutex.Unlock()

	// 双检锁
	if p, exists := m.partitions[partitionID]; exists {
		return p
	}

	p := &timePartition{
		id:        partitionID,
		tsObjects: make(map[uint32]*tsobject.TSObject),
		lastUsed:  time.Now(),
		startTime: time.Now(),
		Duration:  2,
	}
	m.partitions[partitionID] = p
	level.Info(m.logger).Log("msg", "created new partition", "partition", partitionID)
	return p
}

// Ingest 数据写入入口
func (m *CloudTSManager) Ingest(ctx context.Context, series []cortexpb.TimeSeries) error {
	if len(series) == 0 {
		return nil
	}

	// 确定分区 (使用第一个样本的时间戳)
	partitionTime := time.UnixMilli(series[0].Samples[0].TimestampMs).UTC()
	partitionID := uint64(partitionTime.Unix())
	partition := m.getOrCreatePartition(partitionID)

	// 序列化处理单个分区
	partition.mu.Lock()
	defer partition.mu.Unlock()

	// 1. 收集所有标签对
	tagPairs := make([]string, 0, len(series)*3)
	for _, s := range series {
		for _, l := range s.Labels {
			tagPairs = append(tagPairs, l.Name+"="+l.Value)
		}
	}

	// 2. 批量获取或创建标签编码
	encodings := m.globalTagDict.BatchGetOrCreate(tagPairs)

	// 3. 构建分区元数据
	partition.tagArray = tagarray.NewFromEncodings(partitionID, encodings)
	partition.ttMapping = ttmapping.NewFromTadDictTagArrayAndIngester(m.globalTagDict, partition.tagArray, series)

	// 这个函数很奇怪
	// func GroupSeries(tm *TTMapping, ta *tagarray.TagArray) []*SeriesGroup {}
	// 实现时序分组逻辑

	// // 4. 分组并构建TSObjects
	// groups := ttmapping.GroupSeries(partition.ttMapping, partition.tagArray)
	// objects := make(map[uint32]*tsobject.TSObject, len(groups))

	// for _, group := range groups {
	// 	builder := tsobject.NewTSObjectBuilder(group.ID)
	// 	for _, seriesID := range group.SeriesRefs {
	// 		// 转换Cortex时序数据为chunks
	// 		chunks := convertToChunks(series[seriesID-1]) // seriesID是从1开始的索引
	// 		for _, chk := range chunks {
	// 			if err := builder.AddChunk(seriesID, chk); err != nil {
	// 				return fmt.Errorf("add chunk failed: %w", err)
	// 			}
	// 		}
	// 	}

	// 	obj, err := builder.Build()
	// 	if err != nil {
	// 		return fmt.Errorf("build TSObject failed: %w", err)
	// 	}
	// 	objects[group.ID] = obj
	// }

	// 4. 使用优化后的分组算法
	groups, stats := ttmapping.GroupSeriesByCardinality(partition.ttMapping, partition.tagArray)
	level.Debug(m.logger).Log("msg", "series grouping stats",
		"total_series", stats.TotalSeries,
		"group_count", stats.GroupCount,
		"avg_per_group", stats.AvgSeriesPerGroup,
		"min_size", stats.MinGroupSize,
		"max_size", stats.MaxGroupSize)

	// 5. 分组并构建TSObjects
	objects := make(map[uint32]*tsobject.TSObject, len(groups))
	for _, group := range groups {
		builder := tsobject.NewTSObjectBuilder(group.ID)

		// 按时间排序系列以保证TSObject内数据有序
		sortedSeries := make([]uint64, len(group.SeriesRefs))
		copy(sortedSeries, group.SeriesRefs)
		sort.Slice(sortedSeries, func(i, j int) bool {
			return getSeriesMinTime(series[sortedSeries[i]-1]) < getSeriesMinTime(series[sortedSeries[j]-1])
		})

		for _, seriesID := range sortedSeries {
			// 转换Cortex时序数据为chunks
			s := series[seriesID-1] // seriesID是从1开始的索引
			chunks := convertToChunkMetas(s)

			for _, chk := range chunks {
				if err := builder.AddChunk(uint32(seriesID), chk); err != nil {
					return fmt.Errorf("add chunk failed for series %d: %w", seriesID, err)
				}
			}
		}

		obj, err := builder.Build()
		if err != nil {
			return fmt.Errorf("build TSObject for group %d failed: %w", group.ID, err)
		}
		objects[group.ID] = obj

		level.Debug(m.logger).Log("msg", "built TSObject",
			"group_id", group.ID,
			"series_count", len(group.SeriesRefs),
			"common_tags", len(group.CommonTags))

		// 重置builder以便重用
		builder.Reset()
		// tsobject.PutBuilder(builder) // 如果实现了对象池
	}

	// 5. 上传整个分区
	if err := m.cloudWriter.UploadPartition(ctx, partitionID, m.globalTagDict, partition.tagArray, partition.ttMapping, objects); err != nil {
		return fmt.Errorf("upload partition failed: %w", err)
	}

	// 6. 更新内存缓存
	for groupID, obj := range objects {
		partition.tsObjects[groupID] = obj
	}

	return nil
}

// Query 数据查询入口
func (m *CloudTSManager) Query(
	ctx context.Context,
	matchers []*labels.Matcher,
	start, end time.Time,
) ([]*tsobject.TSObject, error) {
	// 1. 确定查询涉及的分区
	partitions := m.getQueryPartitions(start, end)
	if len(partitions) == 0 {
		return nil, nil
	}

	// 2. 并行查询所有分区
	var (
		wg     sync.WaitGroup
		result = make(chan partitionResult, len(partitions))
	)

	for _, part := range partitions {
		wg.Add(1)
		go func(p *timePartition) {
			defer wg.Done()
			objs, err := m.queryPartition(ctx, p, matchers)
			result <- partitionResult{objs: objs, err: err}
		}(part)
	}

	// 3. 收集结果
	go func() {
		wg.Wait()
		close(result)
	}()

	var (
		objects []*tsobject.TSObject
		lastErr error
	)

	for res := range result {
		if res.err != nil {
			lastErr = res.err
			continue
		}
		objects = append(objects, res.objs...)
	}

	return objects, lastErr
}

// queryPartition 查询单个分区
func (m *CloudTSManager) queryPartition(
	ctx context.Context,
	part *timePartition,
	matchers []*labels.Matcher,
) ([]*tsobject.TSObject, error) {
	part.mu.Lock()
	defer part.mu.Unlock()

	// 使用CloudQuerier查询分区
	return m.cloudQuerier.QueryPartition(ctx, part.id, matchers)
}

// getQueryPartitions 获取查询涉及的分区
func (m *CloudTSManager) getQueryPartitions(start, end time.Time) []*timePartition {
	m.partitionMutex.RLock()
	defer m.partitionMutex.RUnlock()

	var partitions []*timePartition
	current := start

	for current.Before(end) {
		partID := uint64(current.Unix())
		if part, exists := m.partitions[partID]; exists {
			partitions = append(partitions, part)
		}
		current = current.Add(time.Hour)
	}

	return partitions
}

// 辅助类型和函数
type partitionResult struct {
	objs []*tsobject.TSObject
	err  error
}

// getSeriesMinTime 获取系列的最早时间戳
func getSeriesMinTime(series cortexpb.TimeSeries) int64 {
	if len(series.Samples) == 0 {
		return 0
	}
	min := series.Samples[0].TimestampMs
	for _, s := range series.Samples[1:] {
		if s.TimestampMs < min {
			min = s.TimestampMs
		}
	}
	return min
}

// convertToChunkMetas 将Cortex时序数据转换为chunks.Meta格式
func convertToChunkMetas(series cortexpb.TimeSeries) []chunks.Meta {
	if len(series.Samples) == 0 {
		return nil
	}

	// 创建Prometheus XOR压缩chunk
	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	if err != nil {
		return nil
	}

	// 添加所有样本
	for _, s := range series.Samples {
		appender.Append(s.TimestampMs, s.Value)
	}

	// 计算时间范围
	minTime := series.Samples[0].TimestampMs
	maxTime := series.Samples[len(series.Samples)-1].TimestampMs

	return []chunks.Meta{
		{
			MinTime: minTime,
			MaxTime: maxTime,
			Chunk:   chunk,
		},
	}
}

// GetPartitionForTime 获取指定时间对应的分区（保持原有实现逻辑）
func (m *CloudTSManager) GetPartitionForTime(t time.Time) *timePartition {
	m.partitionMutex.RLock()
	defer m.partitionMutex.RUnlock()

	// 线性扫描所有分区查找包含该时间点的分区
	for _, partition := range m.partitions {
		if partition.Contains(t) {
			// 更新最后访问时间
			partition.mu.Lock()
			partition.lastUsed = time.Now()
			partition.mu.Unlock()
			return partition
		}
	}
	return nil
}

// Contains 检查分区是否包含特定时间点
func (p *timePartition) Contains(t time.Time) bool {
	start := p.startTime
	end := p.startTime
	for range p.Duration {
		end.Add(time.Hour)
	}
	return !t.Before(start) && t.Before(end)
}

// 添加全局TagDict的访问方法
func (m *CloudTSManager) GetGlobalTagDict() *tagdict.TagDict {
	return m.globalTagDict
}
