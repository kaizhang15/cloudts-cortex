// pkg/cloudts/tagdict/restore.go
package tagdict

import (
	"strings"
	"github.com/golang/protobuf/proto"              // Protobuf 序列化
	"github.com/kaizhang15/cortex-cloudts/pkg/pb"	// pb
)

func LoadTagDictFromSnapshot(data []byte) (*TagDict, error) {
    // 1. 反序列化
    var snapshot pb.TagDictSnapshot
    if err := proto.Unmarshal(data, &snapshot); err != nil {
        return nil, err
    }

    // 2. 重建 Trie 树
    td := NewTagDict()
    td.nextEncoding = snapshot.NextEncoding

    // 3. 重建节点
    // nodeMap := make(map[uint32]*TrieNode) // encoding -> node
    for _, pbNode := range snapshot.Nodes {
        node := &TrieNode{
            Encoding: pbNode.Encoding,
            FullPath: pbNode.FullPath,
            NodeType: NodeType(pbNode.Type),
            Children: make(map[string]*TrieNode),
        }
        
        // 重建父子关系
        if pbNode.Encoding == 0 { // root 节点
            td.root = node
        } else {
            // 这里需要根据 path_fragment 重建层级关系
            // 简化示例：实际需要实现路径解析逻辑
            parent := findParentNode(td.root, pbNode.PathFragment) 
            parent.Children[pbNode.PathFragment] = node
        }

        // 更新反向索引
        if node.NodeType == METRIC {
            td.reverseIndex.metrics[node.Encoding] = node.FullPath
        } else if node.NodeType == TAG_VALUE {
            td.reverseIndex.tags[node.Encoding] = node.FullPath
        }
    }

    return td, nil
}

// findParentNode 根据路径片段查找父节点
func findParentNode(root *TrieNode, pathFragment string) *TrieNode {
    // 特殊情况：空路径或根节点
    if pathFragment == "" || root == nil {
        return root
    }

    // 将路径片段拆分为多个部分（根据实际存储的路径分隔符）
    // 例如："host/node01" -> ["host", "node01"]
    parts := strings.Split(pathFragment, "/")
    if len(parts) == 0 {
        return root
    }

    // 从根节点开始逐级查找
    current := root
    for i := 0; i < len(parts)-1; i++ { // 最后一部分是当前节点自己的片段
        part := parts[i]
        if child, exists := current.Children[part]; exists {
            current = child
        } else {
            // 如果路径不存在，创建一个临时节点（根据业务需求调整）
            newChild := &TrieNode{
                FullPath: part,
                Children:     make(map[string]*TrieNode),
            }
            current.Children[part] = newChild
            current = newChild
        }
    }

    return current
}


// 与ingester集成

// // 在 Ingester 刷盘时触发保存
// // pkg/ingester/flush.go
// func (i *Ingester) flushChunks() error {
//     // 1. 常规刷盘逻辑...
    
//     // 2. 持久化 TagDict
//     if i.cfg.TagDict.PersistEnabled {
//         err := i.tagDict.UploadToS3(
//             i.s3Client, 
//             i.cfg.Storage.Bucket,
//             "tagdict",
//             i.currentPartition,
//         )
//         if err != nil {
//             log.Errorf("Failed to persist TagDict: %v", err)
//         }
//     }
// }

// // 启动时恢复状态
// // pkg/ingester/ingester.go
// func NewIngester(cfg Config) (*Ingester, error) {
//     // 1. 初始化 TagDict
//     td := tagdict.NewTagDict()

//     // 2. 从 S3 加载最新快照（如果启用持久化）
//     if cfg.TagDict.RestoreEnabled {
//         latestSnapshot, err := downloadLatestSnapshot(cfg)
//         if err == nil {
//             if restored, err := tagdict.LoadTagDictFromSnapshot(latestSnapshot); err == nil {
//                 td = restored
//             }
//         }
//     }
    
//     // ...其他初始化逻辑
// }


// // cortex 配置
// tagdict:
//   persist:
//     enabled: true
//     s3_path: "tagdict/snapshots/"  # S3存储前缀
//     local_cache: "/tmp/tagdict"    # 本地临时目录
//   restore_on_startup: true         # 启动时恢复