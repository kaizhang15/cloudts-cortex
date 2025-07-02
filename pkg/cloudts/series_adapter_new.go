package cloudts

import (
	"github.com/go-kit/log"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tagdict"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tsobject"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/ttmapping"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
)

type seriesSetAdapter struct {
	objects   []*tsobject.TSObject
	tagDict   *tagdict.TagDict
	ttMapping *ttmapping.TTMapping
	logger    log.Logger
	idx       int
	err       error
}

func newSeriesSetAdapter(
	objects []*tsobject.TSObject,
	td *tagdict.TagDict,
	tm *ttmapping.TTMapping,
	logger log.Logger,
) storage.SeriesSet {
	return &seriesSetAdapter{
		objects:   objects,
		tagDict:   td,
		ttMapping: tm,
		logger:    logger,
		idx:       -1,
	}
}

func (a *seriesSetAdapter) Next() bool {
	if a.err != nil {
		return false
	}
	a.idx++
	return a.idx < len(a.objects)
}

func (a *seriesSetAdapter) At() storage.Series {
	if a.idx < 0 || a.idx >= len(a.objects) {
		return nil
	}
	return newSeriesAdapter(a.objects[a.idx], a.tagDict, a.ttMapping, a.logger)
}

func (a *seriesSetAdapter) Err() error {
	return a.err
}

// func (a *seriesSetAdapter) Warnings() storage.Warnings {
// 	return nil
// }

func (a *seriesSetAdapter) Warnings() annotations.Annotations {
	return nil
}

type seriesAdapter struct {
	obj       *tsobject.TSObject
	tagDict   *tagdict.TagDict
	ttMapping *ttmapping.TTMapping
	logger    log.Logger
	seriesID  uint32
	lbls      labels.Labels
	lblsInit  bool
}

func newSeriesAdapter(
	obj *tsobject.TSObject,
	td *tagdict.TagDict,
	tm *ttmapping.TTMapping,
	logger log.Logger,
) *seriesAdapter {
	var seriesID uint32
	if len(obj.Header.ChunkRefs) > 0 {
		seriesID = obj.Header.ChunkRefs[0].SeriesRef
	}

	return &seriesAdapter{
		obj:       obj,
		tagDict:   td,
		ttMapping: tm,
		logger:    logger,
		seriesID:  seriesID,
	}
}

func (s *seriesAdapter) Labels() labels.Labels {
	if !s.lblsInit {
		s.lbls = s.loadLabels()
		s.lblsInit = true
	}
	return s.lbls
}

func (s *seriesAdapter) loadLabels() labels.Labels {
	if s.ttMapping == nil || s.tagDict == nil {
		return nil
	}

	tagEncodings := s.ttMapping.GetTagsBySeries(uint64(s.seriesID))
	if len(tagEncodings) == 0 {
		return nil
	}

	lbls := make(labels.Labels, 0, len(tagEncodings))
	for _, enc := range tagEncodings {
		tagPair, ok := s.tagDict.GetTagString(enc)
		if !ok {
			continue
		}

		name, value := parseTagPair(tagPair)
		if name == "" {
			continue
		}

		lbls = append(lbls, labels.Label{
			Name:  name,
			Value: value,
		})
	}

	return lbls
}

func parseTagPair(tagPair string) (name, value string) {
	for i := 0; i < len(tagPair); i++ {
		if tagPair[i] == '=' {
			return tagPair[:i], tagPair[i+1:]
		}
	}
	return "", ""
}

func (s *seriesAdapter) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	if iter, ok := it.(*seriesIterator); ok {
		// 重用传入的迭代器
		iter.reset(s.obj)
		return iter
	}
	// 创建新的迭代器
	return newSeriesIterator(s.obj)
}

// seriesIterator 实现 chunkenc.Iterator 接口
type seriesIterator struct {
	obj       *tsobject.TSObject
	chunkIdx  int
	currChunk chunkenc.Chunk
	currIter  chunkenc.Iterator
	err       error
}

func newSeriesIterator(obj *tsobject.TSObject) chunkenc.Iterator {
	return &seriesIterator{
		obj:      obj,
		chunkIdx: -1,
	}
}

func (it *seriesIterator) Next() chunkenc.ValueType {
	// 如果当前迭代器存在
	if it.currIter != nil {
		// 尝试获取下一个值
		valType := it.currIter.Next()
		if valType != chunkenc.ValNone {
			return valType
		}
		// 当前chunk迭代完毕
		it.currIter = nil
	}

	// 移动到下一个chunk
	it.chunkIdx++
	if it.chunkIdx >= len(it.obj.Header.ChunkRefs) {
		return chunkenc.ValNone
	}

	// 初始化新的chunk和迭代器
	ref := it.obj.Header.ChunkRefs[it.chunkIdx]
	chunkData := it.obj.RawData[ref.DataOffset : ref.DataOffset+ref.DataLength]

	var err error
	it.currChunk, err = chunkenc.FromData(ref.Encoding, chunkData)
	if err != nil {
		it.err = err
		return chunkenc.ValNone
	}

	it.currIter = it.currChunk.Iterator(nil)
	return it.currIter.Next()
}

func (it *seriesIterator) Seek(t int64) chunkenc.ValueType {
	// 简单实现：顺序查找直到找到>=t的点
	for {
		valType := it.Next()
		if valType == chunkenc.ValNone {
			return chunkenc.ValNone
		}
		ts, _ := it.At()
		if ts >= t {
			return valType
		}
	}
}

// func (it *seriesIterator) Seek(t int64, help int) (int64, error) {
//     // 简单实现：顺序查找直到找到>=t的点
//     for {
//         valType := it.Next()
//         if valType == chunkenc.ValNone {
//             return 0, it.Err()
//         }
//         ts, _ := it.At()
//         if ts >= t {
//             return ts, nil
//         }
//     }
// }

func (it *seriesIterator) At() (int64, float64) {
	if it.currIter == nil {
		return 0, 0
	}
	return it.currIter.At()
}

func (it *seriesIterator) AtHistogram() (int64, *histogram.Histogram) {
	if it.currIter == nil {
		return 0, nil
	}
	return it.currIter.AtHistogram()
}

func (it *seriesIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	if it.currIter == nil {
		return 0, nil
	}
	return it.currIter.AtFloatHistogram()
}

func (it *seriesIterator) AtT() int64 {
	if it.currIter == nil {
		return 0
	}
	t, _ := it.At()
	return t
}

func (it *seriesIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	if it.currIter != nil {
		return it.currIter.Err()
	}
	return nil
}

// reset 重置迭代器以重用
func (it *seriesIterator) reset(obj *tsobject.TSObject) {
	it.obj = obj
	it.chunkIdx = -1
	it.currChunk = nil
	it.currIter = nil
	it.err = nil
}
