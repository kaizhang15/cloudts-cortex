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
	"github.com/prometheus/prometheus/storage"
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
	partitionID := uint64(partitionTime.Truncate(time.Hour).Unix())
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
	current := start.Truncate(time.Hour)

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

// CortexTSDBAdapter 实现Cortex存储接口
type CortexTSDBAdapter struct {
	mgr    *CloudTSManager
	logger log.Logger
}

func NewCortexAdapter(mgr *CloudTSManager, logger log.Logger) *CortexTSDBAdapter {
	return &CortexTSDBAdapter{
		mgr:    mgr,
		logger: logger,
	}
}

func (a *CortexTSDBAdapter) Select(
	matchers []*labels.Matcher,
	start, end time.Time,
) (storage.SeriesSet, error) {
	objs, err := a.mgr.Query(context.Background(), matchers, start, end)
	if err != nil {
		return nil, err
	}
	return newSeriesSetAdapter(objs), nil
}

// seriesSetAdapter implements storage.SeriesSet for []*tsobject.TSObject
type seriesSetAdapter struct {
	objects []*tsobject.TSObject
	idx     int
}

func newSeriesSetAdapter(objects []*tsobject.TSObject) storage.SeriesSet {
	return &seriesSetAdapter{
		objects: objects,
		idx:     -1, // Start before the first element
	}
}

// Next moves to the next series (required by storage.SeriesSet)
func (a *seriesSetAdapter) Next() bool {
	a.idx++
	return a.idx < len(a.objects)
}

// At returns the current series (required by storage.SeriesSet)
func (a *seriesSetAdapter) At() storage.Series {
	if a.idx < 0 || a.idx >= len(a.objects) {
		return nil
	}
	return newSeriesAdapter(a.objects[a.idx])
}

// Err returns any iteration error (required by storage.SeriesSet)
func (a *seriesSetAdapter) Err() error {
	return nil // Assuming no error during iteration
}

// seriesAdapter implements storage.Series for a single TSObject
type seriesAdapter struct {
	obj *tsobject.TSObject
}

func newSeriesAdapter(obj *tsobject.TSObject) storage.Series {
	return &seriesAdapter{obj: obj}
}

// Labels returns the Prometheus labels for the series
func (s *seriesAdapter) Labels() labels.Labels {
	// TODO: Convert TSObject tags to Prometheus labels
	// Example (adjust based on your TSObject structure):
	return labels.FromStrings(
		"__name__", s.obj.MetricName(),
		"instance", s.obj.Instance(),
	)
}

// Iterator returns a chunk iterator for the series data
func (s *seriesAdapter) Iterator() storage.SeriesIterator {
	// TODO: Convert TSObject data into a Prometheus SeriesIterator
	// This depends on how your TSObject stores samples.
	return newSeriesIterator(s.obj)
}

// seriesIterator implements storage.SeriesIterator for TSObject data
type seriesIterator struct {
	obj     *tsobject.TSObject
	pos     int
	samples []sample // Assume sample is {t int64, v float64}
}

func newSeriesIterator(obj *tsobject.TSObject) storage.SeriesIterator {
	// TODO: Extract samples from TSObject into []sample
	return &seriesIterator{
		obj:     obj,
		samples: convertToSamples(obj), // Implement this
	}
}

func (it *seriesIterator) Next() bool {
	it.pos++
	return it.pos < len(it.samples)
}

func (it *seriesIterator) At() (int64, float64) {
	return it.samples[it.pos].t, it.samples[it.pos].v
}

func (it *seriesIterator) Err() error {
	return nil
}

// Helper type for sample storage
type sample struct {
	t int64   // timestamp
	v float64 // value
}
