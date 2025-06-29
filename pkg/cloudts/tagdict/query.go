package tagdict

import (
	// "fmt"
	"strings"
	// "sync"
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