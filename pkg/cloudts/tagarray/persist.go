package tagarray

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/protobuf/proto"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/pb"
)

func (ta *TagArray) Save(path string) error {
	ta.lock.RLock()
	defer ta.lock.RUnlock()

	file, err := os.Create(filepath.Join(path, "tagarray.bin"))
	if err != nil {
		return err
	}
	defer file.Close()

	return gob.NewEncoder(file).Encode(ta)
}

func Load(path string) (*TagArray, error) {
	file, err := os.Open(filepath.Join(path, "tagarray.bin"))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var ta TagArray
	if err := gob.NewDecoder(file).Decode(&ta); err != nil {
		return nil, err
	}
	return &ta, nil
}

// ToProto 转换为Protobuf格式
func (ta *TagArray) ToProto() *pb.TagArraySnapshot {
	ta.lock.RLock()
	defer ta.lock.RUnlock()

	pbTA := &pb.TagArraySnapshot{
		PartitionId: ta.PartitionID,
	}

	for _, tf := range ta.TagFreqs {
		pbTA.TagEncodings = append(pbTA.TagEncodings, tf.Encoding)
		pbTA.TagCounts = append(pbTA.TagCounts, uint32(tf.Count))
	}

	return pbTA
}

// UploadToS3 上传TagArray到S3
func (ta *TagArray) UploadToS3(s3Client *s3.S3, bucket string) error {
	// 1. 序列化为Protobuf
	data, err := proto.Marshal(ta.ToProto())
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	// 2. 直接上传到S3 (不经过本地文件)
	key := fmt.Sprintf("tagarrays/%d/%d.bin", ta.PartitionID, time.Now().UnixNano())
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})

	return err
}

// DownloadFromS3 从S3下载并恢复TagArray
func DownloadFromS3(ctx context.Context, s3Client *s3.S3, bucket string, partitionID uint64) (*TagArray, error) {
	// 1. 查找最新的快照
	prefix := fmt.Sprintf("tagarrays/%d/", partitionID)
	listResp, err := s3Client.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("list objects failed: %w", err)
	}

	if len(listResp.Contents) == 0 {
		return nil, fmt.Errorf("no tagarray found for partition %d", partitionID)
	}

	// 获取最新的文件
	var latestKey string
	var latestTime time.Time
	for _, obj := range listResp.Contents {
		if obj.LastModified.After(latestTime) {
			latestKey = *obj.Key
			latestTime = *obj.LastModified
		}
	}

	// 2. 下载文件
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
		return nil, fmt.Errorf("read body failed: %w", err)
	}

	// 3. 反序列化
	return Deserialize(data)
}

// Deserialize 从字节数组恢复TagArray
func Deserialize(data []byte) (*TagArray, error) {
	var pbTA pb.TagArraySnapshot
	if err := proto.Unmarshal(data, &pbTA); err != nil {
		return nil, fmt.Errorf("unmarshal failed: %w", err)
	}

	return NewFromProto(&pbTA), nil
}

// NewFromProto 从Protobuf创建TagArray
func NewFromProto(pbTA *pb.TagArraySnapshot) *TagArray {
	ta := &TagArray{
		PartitionID: pbTA.PartitionId,
		CreatedAt:   time.Now(),
		TagFreqs:    make([]TagFrequency, 0, len(pbTA.TagEncodings)),
	}

	for i := 0; i < len(pbTA.TagEncodings) && i < len(pbTA.TagCounts); i++ {
		ta.TagFreqs = append(ta.TagFreqs, TagFrequency{
			Encoding: pbTA.TagEncodings[i],
			Count:    int(pbTA.TagCounts[i]),
		})
	}

	return ta
}
