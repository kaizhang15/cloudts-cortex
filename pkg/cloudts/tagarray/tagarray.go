// package tagarray

// import (
// 	"sort"
// 	"sync"
// 	"time"

// 	// "github.com/cortexproject/cortex/pkg/ingester/client"
// 	"github.com/cortexproject/cortex/pkg/cortexpb"
// 	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tagdict"
// )

// type TagArray struct {
// 	PartitionID uint64
// 	CreatedAt   time.Time
// 	TagFreqs    []TagFrequency // 按频率排序的标签编码
// 	lock        sync.RWMutex
// }

// // TagFrequency 只需要存储编码和出现次数
// type TagFrequency struct {
// 	Encoding uint32
// 	Count    int
// }

// func NewFromCortexIngester(partitionID uint64, series []client.TimeSeries, td *tagdict.TagDict) *TagArray {
// 	ta := &TagArray{
// 		PartitionID: partitionID,
// 		CreatedAt:   time.Now(),
// 	}
// 	ta.build(series, td)
// 	return ta
// }

// func (ta *TagArray) build(series []client.TimeSeries, td *tagdict.TagDict) {
// 	ta.lock.Lock()
// 	defer ta.lock.Unlock()

// 	// 统计标签频率
// 	freqMap := make(map[uint32]int)
// 	for _, s := range series {
// 		for _, lbl := range s.Labels {
// 			enc, ok := td.GetEncoding(lbl.Name + "=" + lbl.Value)
// 			if !ok {
// 				continue
// 			}
// 			freqMap[enc]++
// 		}
// 	}

// 	// 转换为切片并排序
// 	ta.TagFreqs = make([]TagFrequency, 0, len(freqMap))
// 	for enc, count := range freqMap {
// 		ta.TagFreqs = append(ta.TagFreqs, TagFrequency{enc, count})
// 	}
// 	sort.Slice(ta.TagFreqs, func(i, j int) bool {
// 		return ta.TagFreqs[i].Count > ta.TagFreqs[j].Count
// 	})
// }


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