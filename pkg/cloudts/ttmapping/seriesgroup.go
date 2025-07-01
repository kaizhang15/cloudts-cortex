package ttmapping

import (
	"math"
	"sort"

	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tagarray"
)

// SeriesGroup 表示一个时序数据分组
type SeriesGroup struct {
	ID           uint32   // 分组唯一标识
	SeriesRefs   []uint64 // 包含的时序ID列表
	CommonTags   []uint32 // 该组共享的标签编码
	ExclusiveTag uint32   // 互斥标签编码（如果有）
}

// GroupingStats 分组统计信息
type GroupingStats struct {
	TotalSeries          int     // 总时序数
	GroupCount           int     // 分组数量
	AvgSeriesPerGroup    float64 // 平均每组时序数
	StdDevSeriesPerGroup float64 // 每组时序数的标准差
	MinGroupSize         int     // 最小组大小
	MaxGroupSize         int     // 最大组大小
}

// TagMetric 标签质量指标
type TagMetric struct {
	Encoding     uint32  // 标签编码
	Count        int     // 出现次数
	Cardinality  int     // 不同值数量
	Coverage     float64 // 覆盖时序比例
	Discriminant float64 // 区分度评分
}

// GroupSeriesByCardinality 基于标签基数进行分组
func GroupSeriesByCardinality(tm *TTMapping, ta *tagarray.TagArray) ([]*SeriesGroup, *GroupingStats) {
	// 阶段1：分析标签质量
	tagMetrics := analyzeTagQuality(tm, ta)

	// 阶段2：选择分组标签
	groupingTags := selectGroupingTags(tagMetrics, len(tm.timeseries))

	// 阶段3：构建初始分组
	groups := buildInitialGroups(tm, groupingTags)

	// 阶段4：优化分组结构
	stats := calculateGroupStats(groups)
	optimizedGroups := optimizeGroups(tm, groups, stats)

	return optimizedGroups, stats
}

// --- 核心算法实现 ---

func analyzeTagQuality(tm *TTMapping, ta *tagarray.TagArray) []*TagMetric {
	metrics := make([]*TagMetric, len(ta.TagFreqs))
	totalSeries := len(tm.timeseries)

	for i, tf := range ta.TagFreqs {
		// 精确计算标签基数（通过扫描bitmap）
		cardinality := 0
		valueSet := make(map[uint32]struct{})
		for row := 0; row < tm.bitmap.Rows; row++ {
			if tm.hasTag(row, tf.Encoding) {
				// 假设标签编码结构：前16位是标签名，后16位是值
				baseEnc := tf.Encoding & 0xFFFF0000
				valueSet[baseEnc] = struct{}{}
			}
		}
		cardinality = len(valueSet)

		metrics[i] = &TagMetric{
			Encoding:    tf.Encoding,
			Count:       tf.Count,
			Cardinality: cardinality,
			Coverage:    float64(tf.Count) / float64(totalSeries),
			Discriminant: calculateDiscriminant(
				float64(tf.Count),
				float64(cardinality),
				float64(totalSeries)),
		}
	}

	sort.Slice(metrics, func(i, j int) bool {
		return metrics[i].Discriminant > metrics[j].Discriminant
	})
	return metrics
}

func calculateDiscriminant(count, cardinality, total float64) float64 {
	coverage := count / total
	normCardinality := cardinality / count
	return coverage * (1 - math.Exp(-5*normCardinality))
}

func selectGroupingTags(metrics []*TagMetric, totalSeries int) []*TagMetric {
	var selected []*TagMetric
	coverageSum := 0.0

	for _, m := range metrics {
		// 选择标准：区分度>0.3，覆盖率在5%~95%之间
		if m.Discriminant > 0.3 && m.Coverage > 0.05 && m.Coverage < 0.95 {
			selected = append(selected, m)
			coverageSum += m.Coverage
			if coverageSum >= 0.8 { // 覆盖80%以上时序
				break
			}
		}
	}
	return selected
}

func buildInitialGroups(tm *TTMapping, groupingTags []*TagMetric) map[uint32]*SeriesGroup {
	groups := make(map[uint32]*SeriesGroup)

	// 为每个分组标签创建空组
	for _, tag := range groupingTags {
		groups[tag.Encoding] = &SeriesGroup{
			ID:           tag.Encoding,
			ExclusiveTag: tag.Encoding,
		}
	}

	// 分配时序到组
	for row, seriesID := range tm.timeseries {
		rowTags := tm.getRowTags(row)
		assigned := false

		// 尝试分配到标签组
		for _, tag := range groupingTags {
			if containsTag(rowTags, tag.Encoding) {
				groups[tag.Encoding].SeriesRefs = append(
					groups[tag.Encoding].SeriesRefs,
					seriesID,
				)
				assigned = true
				break
			}
		}

		// 未匹配则放入默认组
		if !assigned {
			if groups[0] == nil {
				groups[0] = &SeriesGroup{ID: 0}
			}
			groups[0].SeriesRefs = append(groups[0].SeriesRefs, seriesID)
		}
	}

	return groups
}

func optimizeGroups(tm *TTMapping, groups map[uint32]*SeriesGroup, stats *GroupingStats) []*SeriesGroup {
	// 计算动态阈值（平均值-标准差，但不小于总数的1%）
	threshold := int(math.Max(
		stats.AvgSeriesPerGroup-stats.StdDevSeriesPerGroup,
		float64(stats.TotalSeries)*0.01,
	))

	var (
		finalGroups []*SeriesGroup
		smallGroups []*SeriesGroup
	)

	// 分离合格组和小型组
	for _, g := range groups {
		if len(g.SeriesRefs) >= threshold {
			finalGroups = append(finalGroups, g)
		} else {
			smallGroups = append(smallGroups, g)
		}
	}

	// 合并小型组
	for _, sg := range smallGroups {
		target := findMergeTarget(sg, finalGroups)
		target.SeriesRefs = append(target.SeriesRefs, sg.SeriesRefs...)
	}

	// 计算公共标签
	for _, g := range finalGroups {
		g.CommonTags = findCommonTags(tm, g)
	}

	return finalGroups
}

// --- 工具函数 ---

func calculateGroupStats(groups map[uint32]*SeriesGroup) *GroupingStats {
	stats := &GroupingStats{}
	if len(groups) == 0 {
		return stats
	}

	var (
		total int
		min   = math.MaxInt32
		max   int
		sum   float64
	)

	// 第一遍计算：基本统计量
	for _, g := range groups {
		size := len(g.SeriesRefs)
		total += size
		sum += float64(size)
		if size < min {
			min = size
		}
		if size > max {
			max = size
		}
	}

	// 第二遍计算：标准差
	avg := sum / float64(len(groups))
	var variance float64
	for _, g := range groups {
		diff := float64(len(g.SeriesRefs)) - avg
		variance += diff * diff
	}
	stdDev := math.Sqrt(variance / float64(len(groups)))

	return &GroupingStats{
		TotalSeries:          total,
		GroupCount:           len(groups),
		AvgSeriesPerGroup:    avg,
		StdDevSeriesPerGroup: stdDev,
		MinGroupSize:         min,
		MaxGroupSize:         max,
	}
}

func findMergeTarget(sg *SeriesGroup, candidates []*SeriesGroup) *SeriesGroup {
	if len(candidates) == 0 {
		return &SeriesGroup{ID: 0}
	}

	// 简化策略：选择最大的候选组
	maxSize := 0
	var target *SeriesGroup
	for _, g := range candidates {
		if len(g.SeriesRefs) > maxSize {
			maxSize = len(g.SeriesRefs)
			target = g
		}
	}
	return target
}

func findCommonTags(tm *TTMapping, group *SeriesGroup) []uint32 {
	if len(group.SeriesRefs) == 0 {
		return nil
	}

	// 步骤1：收集第一个时序的所有标签作为初始公共集
	firstSeriesRow := tm.findSeriesRow(group.SeriesRefs[0])
	if firstSeriesRow == -1 {
		return nil
	}
	commonTags := tm.getRowTags(firstSeriesRow)
	tagSet := make(map[uint32]struct{})
	for _, tag := range commonTags {
		tagSet[tag] = struct{}{}
	}

	// 步骤2：与其他时序求标签交集
	for _, seriesID := range group.SeriesRefs[1:] {
		row := tm.findSeriesRow(seriesID)
		if row == -1 {
			continue
		}

		currentTags := tm.getRowTags(row)
		currentSet := make(map[uint32]struct{})
		for _, tag := range currentTags {
			currentSet[tag] = struct{}{}
		}

		// 求交集
		for tag := range tagSet {
			if _, exists := currentSet[tag]; !exists {
				delete(tagSet, tag)
			}
		}

		if len(tagSet) == 0 {
			break // 无公共标签时提前终止
		}
	}

	// 步骤3：转换为有序切片
	result := make([]uint32, 0, len(tagSet))
	for tag := range tagSet {
		result = append(result, tag)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})

	return result
}

func containsTag(tags []uint32, target uint32) bool {
	for _, t := range tags {
		if t == target {
			return true
		}
	}
	return false
}
