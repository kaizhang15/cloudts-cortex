// pkg/cloudts/cloudwriter.go
package cloudts

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tagarray"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tagdict"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tsobject"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/ttmapping"
	"google.golang.org/protobuf/proto"
)

type CloudWriter struct {
	s3Uploader *s3manager.Uploader
	bucket     string
	logger     log.Logger
	uploadPool chan struct{} // 控制并发上传数
}

func newCloudWriter(s3Config S3Config, logger log.Logger) *CloudWriter {
	sess := newAWSSession(s3Config)
	return &CloudWriter{
		s3Uploader: s3manager.NewUploader(sess),
		bucket:     s3Config.Bucket,
		logger:     logger,
		uploadPool: make(chan struct{}, 10), // 最大10个并发上传
	}
}

// UploadPartition 上传完整分区数据
func (w *CloudWriter) UploadPartition(
	ctx context.Context,
	partitionID uint64,
	td *tagdict.TagDict,
	ta *tagarray.TagArray,
	tm *ttmapping.TTMapping,
	objects map[uint32]*tsobject.TSObject,
) error {
	startTime := time.Now()
	level.Info(w.logger).Log("msg", "start uploading partition", "partition", partitionID)

	// 1. 上传TagArray
	if err := w.uploadTagArray(ctx, partitionID, ta); err != nil {
		return fmt.Errorf("upload TagArray failed: %w", err)
	}

	// 2. 上传TTMapping
	if err := w.uploadTTMapping(ctx, partitionID, tm); err != nil {
		return fmt.Errorf("upload TTMapping failed: %w", err)
	}

	// 3. 并行上传TSObjects
	var wg sync.WaitGroup
	errChan := make(chan error, len(objects))

	for groupID, obj := range objects {
		wg.Add(1)
		go func(gid uint32, o *tsobject.TSObject) {
			defer wg.Done()
			w.uploadPool <- struct{}{}
			defer func() { <-w.uploadPool }()

			if err := w.uploadTSObject(ctx, partitionID, gid, o); err != nil {
				errChan <- fmt.Errorf("upload group %d failed: %w", gid, err)
			}
		}(groupID, obj)
	}

	wg.Wait()
	close(errChan)

	// 检查错误
	if len(errChan) > 0 {
		return <-errChan
	}

	// 4. 上传TagDict快照 (全量)
	if err := w.uploadTagDictSnapshot(ctx, td); err != nil {
		return fmt.Errorf("upload TagDict snapshot failed: %w", err)
	}

	level.Info(w.logger).Log(
		"msg", "partition upload completed",
		"partition", partitionID,
		"duration", time.Since(startTime),
		"objects", len(objects),
	)
	return nil
}

// --- 私有方法 ---

func (w *CloudWriter) uploadTagArray(ctx context.Context, partitionID uint64, ta *tagarray.TagArray) error {
	data, err := proto.Marshal(ta.ToProto())
	if err != nil {
		return err
	}

	key := fmt.Sprintf("partitions/%d/tagarray.bin", partitionID)
	_, err = w.s3Uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (w *CloudWriter) uploadTTMapping(ctx context.Context, partitionID uint64, tm *ttmapping.TTMapping) error {
	data, err := proto.Marshal(tm.ToProto())
	if err != nil {
		return err
	}

	key := fmt.Sprintf("partitions/%d/ttmapping.bin", partitionID)
	_, err = w.s3Uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (w *CloudWriter) uploadTSObject(ctx context.Context, partitionID uint64, groupID uint32, obj *tsobject.TSObject) error {
	data, err := obj.Serialize()
	if err != nil {
		return err
	}

	key := fmt.Sprintf("partitions/%d/objects/%d.bin", partitionID, groupID)
	_, err = w.s3Uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (w *CloudWriter) uploadTagDictSnapshot(ctx context.Context, td *tagdict.TagDict) error {
	data, err := td.Serialize()
	if err != nil {
		return err
	}

	// 使用nextencoding作为版本标识
	version := td.Version()
	key := fmt.Sprintf("global/tagdict/v%s.bin", version)
	_, err = w.s3Uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}
