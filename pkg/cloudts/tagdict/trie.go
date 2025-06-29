package tagdict

import (
	// "fmt"
	// "strings"
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