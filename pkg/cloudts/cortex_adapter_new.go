package cloudts

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tsobject"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	// "github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

type CortexStorageAdapter struct {
	mgr    *CloudTSManager
	logger log.Logger
}

func NewCortexStorageAdapter(mgr *CloudTSManager, logger log.Logger) *CortexStorageAdapter {
	return &CortexStorageAdapter{
		mgr:    mgr,
		logger: logger,
	}
}

func (a *CortexStorageAdapter) Querier(mint, maxt int64) (storage.Querier, error) {
	return &cortexQuerier{
		adapter: a,
		// ctx:     ctx,
		mint: mint,
		maxt: maxt,
	}, nil
}

func (a *CortexStorageAdapter) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return &cortexChunkQuerier{
		adapter: a,
		// ctx:     ctx,
		mint: mint,
		maxt: maxt,
	}, nil
}

func (a *CortexStorageAdapter) StartTime() (int64, error) {
	// 返回存储的最早数据时间戳（Unix毫秒）
	return a.mgr.GetEarliestTimestamp()
}

func (q *CortexStorageAdapter) Close() error {
	return nil
}

func (a *CortexStorageAdapter) getLabelsForSeries(seriesID uint32, partition *timePartition) (labels.Labels, error) {
	// 1. 从TTMapping中获取该时间序列的所有标签编码
	// GetTagsBySeries 方法会返回该seriesID对应的所有标签编码(已加读锁)
	encodings := partition.ttMapping.GetTagsBySeries(uint64(seriesID))
	if len(encodings) == 0 {
		return nil, fmt.Errorf("no tags found for series %d", seriesID)
	}

	// 2. 通过全局标签字典(TagDict)将编码转换为标签键值对
	// 预分配足够空间避免多次扩容
	lbls := make(labels.Labels, 0, len(encodings))

	for _, enc := range encodings {
		// 从标签字典查询编码对应的原始字符串(格式为"name=value")
		tagPair, exists := a.mgr.globalTagDict.GetTagString(enc)
		if !exists {
			// 编码不存在于字典中(可能是字典版本不一致)
			level.Debug(a.logger).Log("msg", "未知的标签编码", "encoding", enc)
			continue
		}

		// 解析标签字符串(格式必须为"name=value")
		parts := strings.SplitN(tagPair, "=", 2)
		if len(parts) != 2 {
			// 格式不合法的标签记录日志但不会报错
			level.Warn(a.logger).Log("msg", "标签格式错误", "tagPair", tagPair)
			continue
		}

		// 构建Prometheus的标准标签结构
		lbls = append(lbls, labels.Label{
			Name:  parts[0], // 标签名
			Value: parts[1], // 标签值
		})
	}

	// 3. 校验至少有一个有效标签
	if len(lbls) == 0 {
		return nil, fmt.Errorf("序列 %d 没有有效的标签", seriesID)
	}

	return lbls, nil
}

// cortexChunkQuerier 实现 storage.ChunkQuerier 接口
type cortexChunkQuerier struct {
	adapter *CortexStorageAdapter
	ctx     context.Context
	mint    int64
	maxt    int64
}

func (q *cortexChunkQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	start := time.UnixMilli(q.mint)
	end := time.UnixMilli(q.maxt)

	// 调用 CloudTSManager 的查询逻辑
	tsObjects, err := q.adapter.mgr.Query(q.ctx, matchers, start, end)
	if err != nil {
		level.Error(q.adapter.logger).Log("msg", "chunk query failed", "err", err)
		return storage.ErrChunkSeriesSet(err)
	}

	// 转换为 Prometheus 的 ChunkSeriesSet
	return newChunkSeriesSetAdapter(tsObjects, q.adapter.logger)
}

func (q *cortexChunkQuerier) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	querier, err := q.adapter.Querier(q.mint, q.maxt)
	if err != nil {
		return nil, nil, err
	}
	defer querier.Close()

	return querier.LabelValues(ctx, name, matchers...)
}

func (q *cortexChunkQuerier) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	querier, err := q.adapter.Querier(q.mint, q.maxt)
	if err != nil {
		return nil, nil, err
	}
	defer querier.Close()

	return querier.LabelNames(ctx, matchers...)
}

func (q *cortexChunkQuerier) Close() error {
	return nil
}

type noopAppender struct{}

func (a *noopAppender) Append(ref storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	return 0, nil
}

func (a *noopAppender) AppendExemplar(ref storage.SeriesRef, l labels.Labels, e exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, nil
}

func (a *noopAppender) Commit() error {
	return nil
}

func (a *noopAppender) Rollback() error {
	return nil
}

func (a *noopAppender) AppendHistogram(ref storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, nil
}

func (a *noopAppender) UpdateMetadata(ref storage.SeriesRef, l labels.Labels, m metadata.Metadata) (storage.SeriesRef, error) {
	return 0, nil
}

func (a *CortexStorageAdapter) Appender(ctx context.Context) storage.Appender {
	return &noopAppender{}
}

type cortexQuerier struct {
	adapter *CortexStorageAdapter
	ctx     context.Context
	mint    int64
	maxt    int64
}

func (q *cortexQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	start := time.UnixMilli(q.mint)
	end := time.UnixMilli(q.maxt)

	tsObjects, err := q.adapter.mgr.Query(q.ctx, matchers, start, end)
	if err != nil {
		level.Error(q.adapter.logger).Log("msg", "query failed", "err", err)
		return storage.ErrSeriesSet(err)
	}

	partition := q.adapter.mgr.GetPartitionForTime(start)
	if partition == nil {
		level.Debug(q.adapter.logger).Log("msg", "no partition found for time range", "start", start)
		return storage.EmptySeriesSet()
	}

	return newSeriesSetAdapter(
		tsObjects,
		q.adapter.mgr.globalTagDict,
		partition.ttMapping,
		q.adapter.logger,
	)
}

func (q *cortexQuerier) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	// 获取指定标签的所有可能值
	values, err := q.adapter.mgr.globalTagDict.GetLabelValues(name)
	if err != nil {
		return nil, nil, err
	}

	// 如果有匹配器，需要过滤结果
	if len(matchers) > 0 {
		filteredValues := make([]string, 0, len(values))
		ss := q.Select(ctx, false, nil, matchers...)

		for ss.Next() {
			series := ss.At()
			for _, lbl := range series.Labels() {
				if lbl.Name == name {
					filteredValues = append(filteredValues, lbl.Value)
				}
			}
		}

		if ss.Err() != nil {
			return nil, nil, ss.Err()
		}
		values = filteredValues
	}

	return values, nil, nil
}

func (q *cortexQuerier) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	names := q.adapter.mgr.globalTagDict.GetLabelNames()

	// 如果有匹配器，需要过滤结果
	if len(matchers) > 0 {
		filteredNames := make(map[string]struct{})
		ss := q.Select(ctx, false, nil, matchers...)

		for ss.Next() {
			for _, lbl := range ss.At().Labels() {
				filteredNames[lbl.Name] = struct{}{}
			}
		}

		if ss.Err() != nil {
			return nil, nil, ss.Err()
		}

		// 转换回切片
		result := make([]string, 0, len(filteredNames))
		for name := range filteredNames {
			result = append(result, name)
		}
		return result, nil, nil
	}

	return names, nil, nil
}

func (q *cortexQuerier) Close() error {
	return nil
}

type chunkSeriesAdapter struct {
	reader     *tsobject.ChunkReader
	seriesID   uint32
	partition  *timePartition
	adapter    *CortexStorageAdapter
	labels     labels.Labels
	labelsOnce sync.Once
}

func (s *chunkSeriesAdapter) Labels() labels.Labels {
	s.labelsOnce.Do(func() {
		var err error
		s.labels, err = s.adapter.getLabelsForSeries(s.seriesID, s.partition)
		if err != nil {
			s.labels = labels.Labels{} // 返回空标签集而不是panic
			level.Warn(s.adapter.logger).Log("msg", "failed to get labels", "series", s.seriesID, "err", err)
		}
	})
	return s.labels
}

func (s *chunkSeriesAdapter) Iterator(it chunks.Iterator) chunks.Iterator {
	s.labelsOnce.Do(func() {
		var err error
		s.labels, err = s.adapter.getLabelsForSeries(s.seriesID, s.partition)
		if err != nil {
			s.labels = labels.Labels{}
			level.Warn(s.adapter.logger).Log("msg", "failed to get labels", "series", s.seriesID, "err", err)
		}
	})

	if it == nil {
		return &chunkIterator{
			reader:    s.reader,
			seriesID:  s.seriesID,
			chunkRefs: nil, // 延迟加载
			idx:       -1,
		}
	}

	// 如果传入了现有的迭代器，可以尝试复用
	if reused, ok := it.(*chunkIterator); ok {
		reused.reader = s.reader
		reused.seriesID = s.seriesID
		reused.chunkRefs = nil
		reused.idx = -1
		reused.err = nil
		reused.current = nil
		return reused
	}

	return &chunkIterator{
		reader:    s.reader,
		seriesID:  s.seriesID,
		chunkRefs: nil,
		idx:       -1,
	}
}

type chunkIterator struct {
	reader    *tsobject.ChunkReader
	seriesID  uint32
	chunkRefs []tsobject.ChunkRef // 延迟加载
	current   chunkenc.Chunk
	idx       int
	err       error
}

func (it *chunkIterator) At() chunks.Meta {
	if it.idx < 0 || it.idx >= len(it.chunkRefs) {
		return chunks.Meta{}
	}

	if it.current == nil {
		ref := it.chunkRefs[it.idx]
		chk, err := it.reader.Chunk(ref)
		if err != nil {
			it.err = err
			return chunks.Meta{}
		}
		it.current = chk
	}

	return chunks.Meta{
		MinTime: it.chunkRefs[it.idx].MinTime,
		MaxTime: it.chunkRefs[it.idx].MaxTime,
		Chunk:   it.current,
	}
}

func (it *chunkIterator) Next() bool {
	it.current = nil // 清除当前chunk

	// 延迟加载chunkRefs
	if it.chunkRefs == nil {
		refs, err := it.reader.Series(it.seriesID)
		if err != nil {
			it.err = err
			return false
		}
		it.chunkRefs = refs
	}

	it.idx++
	return it.idx < len(it.chunkRefs)
}

func (it *chunkIterator) Err() error {
	return it.err
}

func newChunkSeriesSetAdapter(tsObjects []*tsobject.TSObject, logger log.Logger) storage.ChunkSeriesSet {
	if len(tsObjects) == 0 {
		return storage.EmptyChunkSeriesSet()
	}

	var series []storage.ChunkSeries

	for _, obj := range tsObjects {
		reader := tsobject.NewChunkReader(obj)

		// 收集所有唯一的seriesID
		seriesIDs := make(map[uint32]struct{})
		for _, ref := range obj.Header.ChunkRefs {
			seriesIDs[ref.SeriesRef] = struct{}{}
		}

		// 为每个series创建ChunkSeries
		for seriesID := range seriesIDs {
			series = append(series, &chunkSeriesAdapter{
				reader:   reader,
				seriesID: seriesID,
			})
		}
	}

	return &sliceChunkSeriesSet{
		series: series,
		idx:    -1,
	}
}

type sliceChunkSeriesSet struct {
	series   []storage.ChunkSeries
	idx      int
	warnings annotations.Annotations
}

func (s *sliceChunkSeriesSet) Next() bool {
	s.idx++
	return s.idx < len(s.series)
}

func (s *sliceChunkSeriesSet) At() storage.ChunkSeries {
	return s.series[s.idx]
}

func (s *sliceChunkSeriesSet) Err() error {
	return nil
}

func (s *sliceChunkSeriesSet) Warnings() annotations.Annotations {
	return s.warnings
}
