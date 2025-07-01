package ttmapping

import (
	"os"
	"bytes"
	"crypto/md5"
	"fmt"
	"time"
	
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/pb"
	"google.golang.org/protobuf/proto"
	// "github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tagdict"
)

func (m *TTMapping) ToProto() *pb.TTMappingSnapshot {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return &pb.TTMappingSnapshot{
		PartitionId:   m.partition,
		TimeseriesIds: m.timeseries,
		Bitmap: &pb.CompressedBitmap{
			Indices:  m.bitmap.Indices,
			Pointers: int32SliceToProto(m.bitmap.Pointers),
			Rows:     int32(m.bitmap.Rows),
			Cols:     int32(m.bitmap.Cols),
		},
		LastUpdated: m.lastUpdated.UnixNano(),
	}
}

func FromProto(data []byte) (*TTMapping, error) {
	var snapshot pb.TTMappingSnapshot
	if err := proto.Unmarshal(data, &snapshot); err != nil {
		return nil, err
	}

	return &TTMapping{
		partition:  snapshot.PartitionId,
		timeseries: snapshot.TimeseriesIds,
		bitmap: &CompressedBitmap{
			Indices:  snapshot.Bitmap.Indices,
			Pointers: protoInt32SliceToInt(snapshot.Bitmap.Pointers),
			Rows:     int(snapshot.Bitmap.Rows),
			Cols:     int(snapshot.Bitmap.Cols),
		},
		// tagDict:	 td,
		lastUpdated: time.Unix(0, snapshot.LastUpdated),
	}, nil
}

func (m *TTMapping) SaveToFile(path string) error {
	data, err := proto.Marshal(m.ToProto())
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func LoadFromFile(path string) (*TTMapping, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return FromProto(data)
}

func int32SliceToProto(s []int) []int32 {
	result := make([]int32, len(s))
	for i, v := range s {
		result[i] = int32(v)
	}
	return result
}

func protoInt32SliceToInt(s []int32) []int {
	result := make([]int, len(s))
	for i, v := range s {
		result[i] = int(v)
	}
	return result
}


func (m *TTMapping) UploadToS3(s3Client *s3.S3, bucket, prefix string) error {
	data, err := proto.Marshal(m.ToProto())
	if err != nil {
		return err
	}

	key := prefix + "/" + m.getS3Key()
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})
	return err
}

func DownloadFromS3(s3Client *s3.S3, bucket, key string) (*TTMapping, error) {
	out, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(out.Body); err != nil {
		return nil, err
	}

	return FromProto(buf.Bytes())
}

func (m *TTMapping) getS3Key() string {
	hash := md5.Sum([]byte(fmt.Sprintf("%v", m.timeseries)))
	return fmt.Sprintf("%d/%x.bin", m.partition, hash)
}

