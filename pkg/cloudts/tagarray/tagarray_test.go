package tagarray

import (
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tagdict"
	"github.com/stretchr/testify/assert"
)

func createTestTagDict() *tagdict.TagDict {
	td := tagdict.NewTagDict()
	// 按固定顺序插入确保编码可预测
	td.Insert("job=web")      // 预期编码 0
	td.Insert("instance=host1") // 预期编码 1
	td.Insert("zone=sh")       // 预期编码 2
	return td
}

func TestNewFromCortexIngester_Basic(t *testing.T) {
	td := createTestTagDict()
	
	series := []cortexpb.TimeSeries{
		{
			Labels: []cortexpb.LabelAdapter{
				{Name: "job", Value: "web"},
				{Name: "instance", Value: "host1"},
			},
		},
		{
			Labels: []cortexpb.LabelAdapter{
				{Name: "job", Value: "web"},
				{Name: "zone", Value: "sh"},
			},
		},
	}

	ta := NewFromCortexIngester(123, series, td)

	// 验证基础属性
	assert.Equal(t, uint64(123), ta.PartitionID)
	assert.WithinDuration(t, time.Now(), ta.CreatedAt, time.Second)

	// 验证标签频率（按Count排序）
	assert.Len(t, ta.TagFreqs, 3)
	assert.Equal(t, 2, ta.TagFreqs[0].Count) // job=web 出现2次
	assert.Equal(t, 1, ta.TagFreqs[1].Count) // instance=host1
	assert.Equal(t, 1, ta.TagFreqs[2].Count) // zone=sh
}

func TestGetTopNTags(t *testing.T) {
	// _ := createTestTagDict()
	
	// 创建测试数据时明确指定频率顺序
	ta := &TagArray{
		TagFreqs: []TagFrequency{
			{Encoding: 0, Count: 20}, // job=web
			{Encoding: 1, Count: 15},  // instance=host1
			{Encoding: 2, Count: 10}, // zone=sh
		},
	}

	t.Run("获取前2个标签", func(t *testing.T) {
		top := ta.GetTopNTags(2)
		assert.Len(t, top, 2)
		// 只验证频率顺序，不验证具体编码值
		assert.GreaterOrEqual(t, top[0].Count, top[1].Count)
		assert.Equal(t, 20, top[0].Count)
		assert.Equal(t, 15, top[1].Count)
	})
}

func TestGetTagCount(t *testing.T) {
	td := createTestTagDict()
	webEnc, _ := td.GetEncoding("job=web")

	ta := &TagArray{
		TagFreqs: []TagFrequency{
			{Encoding: webEnc, Count: 10},
		},
	}

	t.Run("存在的标签", func(t *testing.T) {
		count, exists := ta.GetTagCount(webEnc)
		assert.True(t, exists)
		assert.Equal(t, 10, count)
	})

	t.Run("不存在的标签", func(t *testing.T) {
		_, exists := ta.GetTagCount(999)
		assert.False(t, exists)
	})
}