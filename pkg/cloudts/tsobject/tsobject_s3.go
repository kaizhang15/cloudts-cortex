package cloudts

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

const (
	s3UploadTimeout    = 5 * time.Minute
	maxS3Retries       = 3
	s3PartSize         = 64 << 20 // 64MB
	defaultS3ACL       = "bucket-owner-full-control"
)

type S3Config struct {
	BucketName    string
	Prefix        string // S3路径前缀 (e.g. "prometheus/cloudts/")
	Region        string
	Endpoint      string // 可选，用于兼容非AWS S3
	S3ForcePathStyle bool
}

type TSObjectUploader struct {
	s3Uploader *s3manager.Uploader
	s3Client   *s3.S3
	cfg        S3Config
	logger     log.Logger
}

func NewTSObjectUploader(cfg S3Config, logger log.Logger) (*TSObjectUploader, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String(cfg.Region),
		Endpoint:         aws.String(cfg.Endpoint),
		S3ForcePathStyle: aws.Bool(cfg.S3ForcePathStyle),
	})
	if err != nil {
		return nil, fmt.Errorf("create S3 session: %w", err)
	}

	return &TSObjectUploader{
		s3Uploader: s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
			u.PartSize = s3PartSize
			u.LeavePartsOnError = false
		}),
		s3Client: s3.New(sess),
		cfg:      cfg,
		logger:   logger,
	}, nil
}

// UploadTSObject 上传TSObject到S3
func (u *TSObjectUploader) UploadTSObject(ctx context.Context, obj *TSObject, blockID string) (string, error) {
	// 1. 生成S3对象Key
	objKey := u.generateObjectKey(blockID, obj.Header.GroupID)

	// 2. 准备上传数据
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		if _, err := obj.WriteTo(pw); err != nil {
			_ = pw.CloseWithError(err)
		}
	}()

	// 3. 执行上传
	ctx, cancel := context.WithTimeout(ctx, s3UploadTimeout)
	defer cancel()

	result, err := u.s3Uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket:      aws.String(u.cfg.BucketName),
		Key:         aws.String(objKey),
		Body:        pr,
		ACL:         aws.String(defaultS3ACL),
		ContentType: aws.String("binary/octet-stream"),
	})

	if err != nil {
		level.Error(u.logger).Log(
			"msg", "failed to upload TSObject",
			"groupID", obj.Header.GroupID,
			"blockID", blockID,
			"err", err,
		)
		return "", fmt.Errorf("S3 upload failed: %w", err)
	}

	level.Debug(u.logger).Log(
		"msg", "successfully uploaded TSObject",
		"location", result.Location,
		"groupID", obj.Header.GroupID,
		"blockSize", len(obj.RawData),
	)

	return objKey, nil
}

// generateObjectKey 生成S3对象路径
// 格式: <prefix>/<blockID>/<groupID>.cldts
func (u *TSObjectUploader) generateObjectKey(blockID string, groupID uint32) string {
	return filepath.Join(
		u.cfg.Prefix,
		blockID,
		fmt.Sprintf("%04x.cldts", groupID), // 16进制格式便于排序
	)
}

// WriteTo 实现io.WriterTo接口（优化内存使用）
func (o *TSObject) WriteTo(w io.Writer) (n int64, err error) {
	// 1. 写入固定头部
	if err := binary.Write(w, binary.LittleEndian, o.Header.Magic); err != nil {
		return n, err
	}
	n += 4

	// 2. 写入动态部分（ChunkRefs）
	for _, ref := range o.Header.ChunkRefs {
		if err := binary.Write(w, binary.LittleEndian, ref); err != nil {
			return n, err
		}
		n += int64(binary.Size(ref))
	}

	// 3. 写入原始数据（流式传输避免内存复制）
	dataWritten, err := w.Write(o.RawData)
	return n + int64(dataWritten), err
}

// 补充：从S3下载TSObject的实现
func (u *TSObjectUploader) DownloadTSObject(ctx context.Context, objKey string) (*TSObject, error) {
	// 实现略（使用s3manager.Downloader）
	// ...
	return nil, nil
}