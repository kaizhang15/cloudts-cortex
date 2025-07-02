package cloudts

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
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

func (a *CortexStorageAdapter) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return &cortexQuerier{
		adapter: a,
		ctx:     ctx,
		mint:    mint,
		maxt:    maxt,
	}, nil
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
	querier, err := q.adapter.Querier(ctx, q.mint, q.maxt)
	if err != nil {
		return nil, nil, err
	}
	defer querier.Close()

	return querier.LabelValues(ctx, name, matchers...)
}

func (q *cortexChunkQuerier) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	querier, err := q.adapter.Querier(ctx, q.mint, q.maxt)
	if err != nil {
		return nil, nil, err
	}
	defer querier.Close()

	return querier.LabelNames(ctx, matchers...)
}

func (q *cortexChunkQuerier) Close() error {
	return nil
}

func (a *CortexStorageAdapter) ChunkQuerier(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
	return &cortexChunkQuerier{
		adapter: a,
		ctx:     ctx,
		mint:    mint,
		maxt:    maxt,
	}, nil
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
