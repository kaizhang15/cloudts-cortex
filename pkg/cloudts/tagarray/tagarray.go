package tagarray

import (
	"sort"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/cortexpb" // 更新为正确的包路径
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tagdict"
)

type TagArray struct {
	PartitionID uint64
	CreatedAt   time.Time
	TagFreqs    []TagFrequency // 按频率排序的标签编码
	lock        sync.RWMutex
}

type TagFrequency struct {
	Encoding uint32
	Count    int
}

func NewFromCortexIngester(partitionID uint64, series []cortexpb.TimeSeries, td *tagdict.TagDict) *TagArray {
	ta := &TagArray{
		PartitionID: partitionID,
		CreatedAt:   time.Now(),
	}
	ta.build(series, td)
	return ta
}

func (ta *TagArray) build(series []cortexpb.TimeSeries, td *tagdict.TagDict) {
	ta.lock.Lock()
	defer ta.lock.Unlock()

	freqMap := make(map[uint32]int)
	for _, s := range series {
		for _, lbl := range s.Labels {
			enc, ok := td.GetEncoding(lbl.Name + "=" + lbl.Value)
			if !ok {
				continue
			}
			freqMap[enc]++
		}
	}

	ta.TagFreqs = make([]TagFrequency, 0, len(freqMap))
	for enc, count := range freqMap {
		ta.TagFreqs = append(ta.TagFreqs, TagFrequency{enc, count})
	}
	sort.Slice(ta.TagFreqs, func(i, j int) bool {
		return ta.TagFreqs[i].Count > ta.TagFreqs[j].Count
	})
}

// TotalSeries 估算该分区的时间序列总数（基于最高频标签）
func (ta *TagArray) TotalSeries() int {
	ta.lock.RLock()
	defer ta.lock.RUnlock()

	if len(ta.TagFreqs) == 0 {
		return 0
	}

	maxCount := 0
	for _, tf := range ta.TagFreqs {
		if tf.Count > maxCount {
			maxCount = tf.Count
		}
	}
	return maxCount
}

// GetFrequency 获取特定标签编码的出现次数
func (ta *TagArray) GetFrequency(enc uint32) int {
	ta.lock.RLock()
	defer ta.lock.RUnlock()

	for _, tf := range ta.TagFreqs {
		if tf.Encoding == enc {
			return tf.Count
		}
	}
	return 0
}

// NewFromEncodings 从标签编码映射创建TagArray
// 参数:
//   - partitionID: 分区唯一标识
//   - encodings: 标签对到编码的映射，格式为 "tagName=tagValue" -> encoding
func NewFromEncodings(partitionID uint64, encodings map[string]uint32) *TagArray {
	ta := &TagArray{
		PartitionID: partitionID,
		CreatedAt:   time.Now(),
	}
	ta.buildFromEncodings(encodings)
	return ta
}

// buildFromEncodings 从编码映射构建TagArray内部结构
func (ta *TagArray) buildFromEncodings(encodings map[string]uint32) {
	ta.lock.Lock()
	defer ta.lock.Unlock()

	// 统计编码频率
	freqMap := make(map[uint32]int)
	for _, enc := range encodings {
		freqMap[enc]++
	}

	// 转换为排序后的TagFrequency切片
	ta.TagFreqs = make([]TagFrequency, 0, len(freqMap))
	for enc, count := range freqMap {
		ta.TagFreqs = append(ta.TagFreqs, TagFrequency{Encoding: enc, Count: count})
	}

	// 按频率降序排序
	sort.Slice(ta.TagFreqs, func(i, j int) bool {
		return ta.TagFreqs[i].Count > ta.TagFreqs[j].Count
	})
}
