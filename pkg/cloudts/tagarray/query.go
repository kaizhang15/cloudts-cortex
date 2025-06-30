package tagarray

import "sort"

// GetTopNTags 获取前N个高频标签
func (ta *TagArray) GetTopNTags(n int) []TagFrequency {
	ta.lock.RLock()
	defer ta.lock.RUnlock()

	if n > len(ta.TagFreqs) {
		n = len(ta.TagFreqs)
	}
	return ta.TagFreqs[:n]
}

// GetTagCount 获取特定标签的出现次数
func (ta *TagArray) GetTagCount(enc uint32) (int, bool) {
	ta.lock.RLock()
	defer ta.lock.RUnlock()

	// 二分查找优化（因为已排序）
	idx := sort.Search(len(ta.TagFreqs), func(i int) bool {
		return ta.TagFreqs[i].Encoding >= enc
	})

	if idx < len(ta.TagFreqs) && ta.TagFreqs[idx].Encoding == enc {
		return ta.TagFreqs[idx].Count, true
	}
	return 0, false
}