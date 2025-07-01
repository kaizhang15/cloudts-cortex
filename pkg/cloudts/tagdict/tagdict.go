package tagdict

import (
	// "fmt"
	// "strings"
	"strings"
	"sync"
)

type NodeType int

const (
	ROOT NodeType = iota
	METRIC
	TAG_NAME
	TAG_VALUE
)

type TrieNode struct {
	Children map[string]*TrieNode
	NodeType NodeType
	Encoding uint32
	FullPath string
}

type TagDict struct {
	root         *TrieNode
	reverseIndex struct {
		metrics map[uint32]string
		tags    map[uint32]string
		lock    sync.RWMutex
	}
	nextEncoding uint32
	lock         sync.RWMutex
}

func NewTagDict() *TagDict {
	return &TagDict{
		root: &TrieNode{
			Children: make(map[string]*TrieNode),
			NodeType: ROOT,
		},
		reverseIndex: struct {
			metrics map[uint32]string
			tags    map[uint32]string
			lock    sync.RWMutex
		}{
			metrics: make(map[uint32]string),
			tags:    make(map[uint32]string),
		},
	}
}

// IterateEncodings 安全遍历所有编码（读锁保护）
func (td *TagDict) IterateEncodings(fn func(enc uint32, tagPair string) bool) {
	td.lock.RLock()
	defer td.lock.RUnlock()

	// 先遍历metrics
	for enc, str := range td.reverseIndex.metrics {
		if !fn(enc, str) {
			return
		}
	}
	// 再遍历tags
	for enc, str := range td.reverseIndex.tags {
		if !fn(enc, str) {
			return
		}
	}
}

func (td *TagDict) Version() uint32 {
	td.lock.RLock()
	defer td.lock.RUnlock()
	return td.nextEncoding
}

// BatchGetOrCreate 批量获取或创建标签编码
func (td *TagDict) BatchGetOrCreate(tagPairs []string) map[string]uint32 {
	td.lock.Lock()
	defer td.lock.Unlock()

	results := make(map[string]uint32, len(tagPairs))

	for _, tagPair := range tagPairs {
		// 先尝试获取已有编码
		if enc, exists := td.getEncodingUnsafe(tagPair); exists {
			results[tagPair] = enc
			continue
		}

		// 不存在则创建新编码
		var enc uint32
		if strings.Contains(tagPair, "=") {
			enc = td.insertTagNameValueUnsafe(tagPair)
		} else {
			enc = td.insertPureMetricUnsafe(tagPair)
		}
		results[tagPair] = enc
	}

	return results
}
