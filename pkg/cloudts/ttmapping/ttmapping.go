package ttmapping

import (
	"fmt"
	"sync"
	"time"

	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/pb"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tagarray"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tagdict"

	// "github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/cortexpb"
)

type CompressedBitmap struct {
	Indices  []uint32 // 非零位的列索引
	Pointers []int    // 行偏移指针
	Rows     int      // 时间序列数量
	Cols     int      // 标签编码数量
}

type TTMapping struct {
	partition   uint64
	timeseries  []uint64
	bitmap      *CompressedBitmap
	tagDict     *tagdict.TagDict
	lastUpdated time.Time
	lastUsed    time.Time
	lock        sync.RWMutex
}

func NewFromDense(dense [][]bool, seriesIDs []uint64) *TTMapping {
	bm := &CompressedBitmap{}
	bm.Compress(dense)
	return &TTMapping{
		timeseries: seriesIDs,
		bitmap:     bm,
	}
}

func (bm *CompressedBitmap) Compress(dense [][]bool) {
	bm.Pointers = append(bm.Pointers, 0)
	for _, row := range dense {
		for col, val := range row {
			if val {
				bm.Indices = append(bm.Indices, uint32(col))
			}
		}
		bm.Pointers = append(bm.Pointers, len(bm.Indices))
	}
	bm.Rows = len(dense)
	if len(dense) > 0 {
		bm.Cols = len(dense[0])
	}
}

// NewFromTagArrayAndIngester 从tagarray和ingester数据构建TTMapping
func NewFromTadDictTagArrayAndIngester(
	td *tagdict.TagDict,
	ta *tagarray.TagArray,
	ingesterSeries []cortexpb.TimeSeries,
) *TTMapping {
	tm := &TTMapping{
		partition:   ta.PartitionID,
		tagDict:     td,
		lastUpdated: time.Now(),
	}
	tm.build(ta, ingesterSeries)
	return tm
}

func (tm *TTMapping) build(ta *tagarray.TagArray, series []cortexpb.TimeSeries) {
	tm.lock.Lock()
	defer tm.lock.Unlock()

	// 1. 生成从1开始的顺序ID
	tm.timeseries = make([]uint64, len(series))
	for i := range series {
		tm.timeseries[i] = uint64(i + 1) // 直接使用索引+1作为ID
	}

	// 2. 创建标签编码到列索引的映射
	encToCol := make(map[uint32]int)
	for col, tf := range ta.TagFreqs {
		encToCol[tf.Encoding] = col
	}

	// 3. 初始化位图 (rows=series数量, cols=标签数量)
	bitmap := make([][]bool, len(series))
	for i := range bitmap {
		bitmap[i] = make([]bool, len(ta.TagFreqs))
	}

	// 4. 填充位图
	for row, s := range series {
		for _, lbl := range s.Labels {
			enc, ok := tm.tagDict.GetEncoding(lbl.Name + "=" + lbl.Value) //tagdict在什么位置？应该作为变量传进来、或者作为全局变量
			if !ok {
				continue
			}
			if col, exists := encToCol[enc]; exists {
				bitmap[row][col] = true
			}
		}
	}

	// 5. 压缩位图
	tm.bitmap = &CompressedBitmap{}
	tm.bitmap.Compress(bitmap)
}

func (bm *CompressedBitmap) FromProto(pbBm *pb.CompressedBitmap) {
	bm.Indices = pbBm.Indices
	bm.Pointers = make([]int, len(pbBm.Pointers))
	for i, p := range pbBm.Pointers {
		bm.Pointers[i] = int(p)
	}
	bm.Rows = int(pbBm.Rows)
	bm.Cols = int(pbBm.Cols)
}

func NewFromProto(snapshot *pb.TTMappingSnapshot, td ...*tagdict.TagDict) *TTMapping {
	bm := &CompressedBitmap{}
	bm.FromProto(snapshot.Bitmap)

	var dict *tagdict.TagDict
	if len(td) > 0 {
		dict = td[0]
	}

	return &TTMapping{
		partition:   snapshot.PartitionId,
		timeseries:  snapshot.TimeseriesIds,
		bitmap:      bm,
		tagDict:     dict,
		lastUpdated: time.Unix(0, snapshot.LastUpdated),
	}
}

func (tm *TTMapping) BindTagDict(td *tagdict.TagDict) error {
	tm.lock.Lock()
	defer tm.lock.Unlock()

	if tm.tagDict != nil {
		return fmt.Errorf("tagdict already bound")
	}
	tm.tagDict = td
	return nil
}

func (tm *TTMapping) LastUsed() time.Time {
	tm.lock.RLock()
	defer tm.lock.RUnlock()
	return tm.lastUsed
}

func (tm *TTMapping) MarkUsed() {
	tm.lock.Lock()
	defer tm.lock.Unlock()
	tm.lastUsed = time.Now()
}

// HasSeriesTag 检查指定时间序列是否包含特定标签
func (m *TTMapping) HasSeriesTag(seriesID uint64, tagEnc uint32) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()

	for i, id := range m.timeseries {
		if id == seriesID {
			return m.hasTag(i, tagEnc)
		}
	}
	return false
}
