// pkg/cloudts/tagdict/persist.go
package tagdict

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/protobuf/proto"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/pb"
)

// Serialize 将TagDict序列化为字节数组
func (td *TagDict) Serialize() ([]byte, error) {
	td.lock.RLock()
	defer td.lock.RUnlock()

	snapshot := &pb.TagDictSnapshot{
		NextEncoding: td.nextEncoding,
	}

	// 1. 序列化Trie树节点
	var traverse func(node *TrieNode)
	traverse = func(node *TrieNode) {
		if node == nil {
			return
		}

		pbNode := &pb.TagDictSnapshot_TrieNode{
			PathFragment: getPathFragment(node.FullPath),
			Encoding:     node.Encoding,
			FullPath:     node.FullPath,
			Type:         pb.NodeType(node.NodeType),
		}
		snapshot.Nodes = append(snapshot.Nodes, pbNode)

		for _, child := range node.Children {
			traverse(child)
		}
	}
	traverse(td.root)

	// 2. 序列化反向索引（转换为partitions）
	// 由于原结构没有分区概念，我们将所有标签视为一个虚拟分区
	virtualPart := &pb.TagDictSnapshot_Partition{
		PartitionId:    0, // 虚拟分区ID
		TagFrequencies: make(map[uint32]uint32),
	}

	// 合并metrics和tags到频率统计
	td.reverseIndex.lock.RLock()
	for enc := range td.reverseIndex.metrics {
		virtualPart.TagFrequencies[enc] = 1 // 默认计数为1
	}
	for enc := range td.reverseIndex.tags {
		if _, exists := virtualPart.TagFrequencies[enc]; !exists {
			virtualPart.TagFrequencies[enc] = 1
		} else {
			virtualPart.TagFrequencies[enc]++
		}
	}
	td.reverseIndex.lock.RUnlock()

	snapshot.Partitions = append(snapshot.Partitions, virtualPart)

	return proto.Marshal(snapshot)
}

// Deserialize 从字节数组恢复TagDict
func (td *TagDict) Deserialize(data []byte) error {
	// 检查空数据
	if len(data) == 0 {
		return fmt.Errorf("empty input data")
	}

	td.lock.Lock()
	defer td.lock.Unlock()

	var snapshot pb.TagDictSnapshot
	if err := proto.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("protobuf unmarshal failed: %w", err)
	}

	// 1. 重置字典
	td.root = &TrieNode{
		Children: make(map[string]*TrieNode),
		NodeType: ROOT,
	}
	td.nextEncoding = snapshot.NextEncoding
	td.reverseIndex = struct {
		metrics map[uint32]string
		tags    map[uint32]string
		lock    sync.RWMutex
	}{
		metrics: make(map[uint32]string),
		tags:    make(map[uint32]string),
	}

	// 2. 重建Trie树并创建编码到路径的映射
	nodeMap := make(map[string]*TrieNode)
	encodingToPath := make(map[uint32]string)

	for _, pbNode := range snapshot.Nodes {
		node := &TrieNode{
			Children: make(map[string]*TrieNode),
			NodeType: NodeType(pbNode.Type),
			Encoding: pbNode.Encoding,
			FullPath: pbNode.FullPath,
		}
		nodeMap[pbNode.FullPath] = node
		encodingToPath[pbNode.Encoding] = pbNode.FullPath
	}

	// 3. 重建父子关系
	for _, node := range nodeMap {
		if node.FullPath == "" {
			td.root = node
			continue
		}

		parentPath := getParentPath(node.FullPath)
		if parent, ok := nodeMap[parentPath]; ok {
			fragment := getPathFragment(node.FullPath)
			parent.Children[fragment] = node
		}
	}

	// 4. 重建反向索引
	for _, part := range snapshot.Partitions {
		for enc := range part.TagFrequencies {
			if fullPath, ok := encodingToPath[enc]; ok {
				if strings.Contains(fullPath, "=") {
					td.reverseIndex.tags[enc] = fullPath
				} else {
					td.reverseIndex.metrics[enc] = fullPath
				}
			}
		}
	}

	return nil
}

// UploadToS3 上传到S3存储
func (td *TagDict) UploadToS3(ctx context.Context, s3Client *s3.S3, bucket string) error {
	data, err := td.Serialize()
	if err != nil {
		return fmt.Errorf("serialization failed: %w", err)
	}

	key := fmt.Sprintf("tagdict/v%d.bin", td.nextEncoding)
	_, err = s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

// DownloadFromS3 从S3下载最新版本
func DownloadFromS3(ctx context.Context, s3Client *s3.S3, bucket string) (*TagDict, error) {
	// 查找最新版本
	listResp, err := s3Client.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String("tagdict/"),
	})
	if err != nil {
		return nil, fmt.Errorf("list objects failed: %w", err)
	}

	if len(listResp.Contents) == 0 {
		return nil, fmt.Errorf("no tagdict found in bucket %s", bucket)
	}

	// 获取版本号最大的文件
	var latestKey string
	var maxVersion uint32
	for _, obj := range listResp.Contents {
		var ver uint32
		if _, err := fmt.Sscanf(*obj.Key, "tagdict/v%d.bin", &ver); err == nil && ver >= maxVersion {
			maxVersion = ver
			latestKey = *obj.Key
		}
	}

	// 下载文件
	resp, err := s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(latestKey),
	})
	if err != nil {
		return nil, fmt.Errorf("download failed: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read failed: %w", err)
	}

	// 反序列化
	td := NewTagDict()
	if err := td.Deserialize(data); err != nil {
		return nil, fmt.Errorf("deserialize failed: %w", err)
	}

	return td, nil
}

// --- Helper Functions ---

func getPathFragment(fullPath string) string {
	if idx := strings.LastIndex(fullPath, "/"); idx != -1 {
		return fullPath[idx+1:]
	}
	return fullPath
}

func getParentPath(fullPath string) string {
	if idx := strings.LastIndex(fullPath, "/"); idx != -1 {
		return fullPath[:idx]
	}
	return ""
}
