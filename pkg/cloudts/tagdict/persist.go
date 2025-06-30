// pkg/cloudts/tagdict/persist_local.go
package tagdict

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"          // AWS S3 客户端
	"github.com/golang/protobuf/proto"              // Protobuf 序列化
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/pb"	// pb
)

func (td *TagDict) SaveSnapshotLocal(path string) error {
    td.lock.RLock()
    defer td.lock.RUnlock()

    // 1. 构建 Protobuf 消息
    snapshot := &pb.TagDictSnapshot{
        NextEncoding: td.nextEncoding,
    }

    // 2. 序列化 Trie 树
    var traverse func(path []string, node *TrieNode)
    traverse = func(path []string, node *TrieNode) {
        if node == nil {
            return
        }
        
        pbNode := &pb.TagDictSnapshot_TrieNode{
            PathFragment: strings.Join(path, ""),
            Encoding:     node.Encoding,
            FullPath:     node.FullPath,
            Type:         pb.NodeType(node.NodeType),
        }
        snapshot.Nodes = append(snapshot.Nodes, pbNode)

        for frag, child := range node.Children {
            traverse(append(path, frag), child)
        }
    }
    traverse(nil, td.root)

    // 3. 写入磁盘
    data, err := proto.Marshal(snapshot)
    if err != nil {
        return err
    }
    return os.WriteFile(path, data, 0644)
}

// pkg/cloudts/tagdict/persist_s3.go
func (td *TagDict) UploadToS3(s3Client *s3.S3, bucket, prefix string, partition uint64) error {
    // 1. 生成临时本地文件
    tmpFile := fmt.Sprintf("/tmp/tagdict-%d-%d.bin", partition, time.Now().Unix())
    if err := td.SaveSnapshotLocal(tmpFile); err != nil {
        return err
    }
    defer os.Remove(tmpFile)

    // 2. 上传到 S3
    file, err := os.Open(tmpFile)
    if err != nil {
        return err
    }
    defer file.Close()

    key := fmt.Sprintf("%s/snapshots/%d/%d.bin", prefix, partition, time.Now().UnixNano())
    _, err = s3Client.PutObject(&s3.PutObjectInput{
        Bucket: &bucket,
        Key:    &key,
        Body:   file,
    })
    return err
}