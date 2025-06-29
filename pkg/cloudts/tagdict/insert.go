package tagdict

import (
	// "fmt"
	"strings"
	// "sync"
)

func (td *TagDict) Insert(tagPair string) uint32 {
	td.lock.Lock()
	defer td.lock.Unlock()

	if strings.Contains(tagPair, "=") {
		return td.insertTagNameValue(tagPair)
	}
	return td.insertPureMetric(tagPair)
}

func (td *TagDict) insertPureMetric(metric string) uint32 {
	if enc, exists := td.getMetricEncoding(metric); exists {
		return enc
	}

	enc := td.nextEncoding
	td.nextEncoding++

	node := &TrieNode{
		NodeType: METRIC,
		FullPath: metric,
		Encoding: enc,
	}
	td.root.Children[metric] = node

	td.reverseIndex.lock.Lock()
	td.reverseIndex.metrics[enc] = metric
	td.reverseIndex.lock.Unlock()

	return enc
}

func (td *TagDict) insertTagNameValue(tagPair string) uint32 {
	if enc, exists := td.getTagEncoding(tagPair); exists {
		return enc
	}

	parts := strings.SplitN(tagPair, "=", 2)
	tagName, tagValue := parts[0], parts[1]

	nameNode, exists := td.root.Children[tagName]
	if !exists {
		nameNode = &TrieNode{
			NodeType: TAG_NAME,
			Children: make(map[string]*TrieNode),
		}
		td.root.Children[tagName] = nameNode
	}

	enc := td.nextEncoding
	td.nextEncoding++

	valueNode := &TrieNode{
		NodeType: TAG_VALUE,
		FullPath: tagPair,
		Encoding: enc,
	}
	nameNode.Children[tagValue] = valueNode

	td.reverseIndex.lock.Lock()
	td.reverseIndex.tags[enc] = tagPair
	td.reverseIndex.lock.Unlock()

	return enc
}