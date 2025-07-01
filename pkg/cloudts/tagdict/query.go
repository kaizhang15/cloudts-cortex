package tagdict

import (
	// "fmt"
	"fmt"
	"strings"

	// "sync"

	"github.com/prometheus/prometheus/model/labels"
)

func (td *TagDict) GetTagString(encoding uint32) (string, bool) {
	td.reverseIndex.lock.RLock()
	defer td.reverseIndex.lock.RUnlock()

	if str, exists := td.reverseIndex.metrics[encoding]; exists {
		return str, true
	}
	if str, exists := td.reverseIndex.tags[encoding]; exists {
		return str, true
	}
	return "", false
}

func (td *TagDict) getMetricEncoding(metric string) (uint32, bool) {
	if node, exists := td.root.Children[metric]; exists && node.NodeType == METRIC {
		return node.Encoding, true
	}
	return 0, false
}

func (td *TagDict) getTagEncoding(tagPair string) (uint32, bool) {
	parts := strings.SplitN(tagPair, "=", 2)
	if len(parts) != 2 {
		return 0, false
	}

	nameNode, exists := td.root.Children[parts[0]]
	if !exists || nameNode.NodeType != TAG_NAME {
		return 0, false
	}

	if valueNode, exists := nameNode.Children[parts[1]]; exists {
		return valueNode.Encoding, true
	}
	return 0, false
}

func (td *TagDict) GetEncoding(tagPair string) (uint32, bool) {
	if strings.Contains(tagPair, "=") {
		return td.getTagEncoding(tagPair)
	}
	return td.getMetricEncoding(tagPair)
}

// func (td *TagDict) ResolveMatchers(matchers []*labels.Matcher) []uint32 {
// 	var encodings []uint32

// 	td.lock.RLock()
// 	defer td.lock.RUnlock()

// 	for _, m := range matchers {
// 		// 构建标签查询字符串
// 		query := m.Name + "=" + m.Value

// 		// 在Trie中查找
// 		if enc, ok := td.GetEncoding(query); ok {
// 			encodings = append(encodings, enc)
// 		}
// 	}

// 	return encodings
// }

func (td *TagDict) ResolveMatchers(matchers []*labels.Matcher) ([]uint32, error) {
	var encodings []uint32

	td.lock.RLock()
	defer td.lock.RUnlock()

	for _, m := range matchers {
		// 验证匹配器类型
		if m.Type != labels.MatchEqual {
			return nil, fmt.Errorf("unsupported matcher type: %v", m.Type)
		}

		query := m.Name + "=" + m.Value
		if enc, ok := td.GetEncoding(query); ok {
			encodings = append(encodings, enc)
		}
	}

	return encodings, nil
}

// getEncodingUnsafe 内部方法，不获取锁（由外部保证）
func (td *TagDict) getEncodingUnsafe(tagPair string) (uint32, bool) {
	if strings.Contains(tagPair, "=") {
		return td.getTagEncoding(tagPair)
	}
	return td.getMetricEncoding(tagPair)
}
