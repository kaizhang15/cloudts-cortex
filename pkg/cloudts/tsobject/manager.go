package tsobject

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

type S3Manager struct {
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	client     *s3.S3
	bucket     string
	prefix     string
	logger     log.Logger
}

func NewS3Manager(cfg S3Config, logger log.Logger) (*S3Manager, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String(cfg.Region),
		Endpoint:         aws.String(cfg.Endpoint),
		S3ForcePathStyle: aws.Bool(cfg.S3ForcePathStyle),
	})
	if err != nil {
		return nil, fmt.Errorf("create S3 session: %w", err)
	}

	return &S3Manager{
		uploader:   s3manager.NewUploader(sess),
		downloader: s3manager.NewDownloader(sess),
		client:     s3.New(sess),
		bucket:     cfg.BucketName,
		prefix:     cfg.Prefix,
		logger:     logger,
	}, nil
}

func (m *S3Manager) Upload(ctx context.Context, obj *TSObject, blockID string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, S3UploadTimeout)
	defer cancel()

	key := m.objectKey(blockID, obj.Header.GroupID)
	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()
		if _, err := obj.WriteToAll(pw); err != nil {
			pw.CloseWithError(err)
		}
	}()

	_, err := m.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket:      aws.String(m.bucket),
		Key:         aws.String(key),
		Body:        pr,
		ACL:         aws.String(DefaultS3ACL),
		ContentType: aws.String("binary/octet-stream"),
	})

	if err != nil {
		level.Error(m.logger).Log("msg", "TSObject upload failed", "key", key, "err", err)
		return "", err
	}

	return key, nil
}

func (m *S3Manager) Download(ctx context.Context, key string) (*TSObject, error) {
	ctx, cancel := context.WithTimeout(ctx, S3DownloadTimeout)
	defer cancel()

	// 先下载头部
	headResp, err := m.client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	// 计算需要下载的数据范围
	headerEnd := int64(HeaderSize)
	if *headResp.ContentLength < headerEnd {
		headerEnd = *headResp.ContentLength
	}

	// 下载头部
	headerBuf := aws.NewWriteAtBuffer(make([]byte, 0, headerEnd))
	if _, err := m.downloader.DownloadWithContext(ctx, headerBuf, &s3.GetObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=0-%d", headerEnd-1)),
	}); err != nil {
		return nil, err
	}

	// 解析头部
	header, err := decodeHeader(bytes.NewReader(headerBuf.Bytes()))
	if err != nil {
		return nil, err
	}

	// 下载数据部分
	dataBuf := aws.NewWriteAtBuffer(make([]byte, 0, header.ChunkRefs[len(header.ChunkRefs)-1].DataOffset+header.ChunkRefs[len(header.ChunkRefs)-1].DataLength))
	if _, err := m.downloader.DownloadWithContext(ctx, dataBuf, &s3.GetObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-", headerEnd)),
	}); err != nil {
		return nil, err
	}

	return &TSObject{
		Header:  *header,
		RawData: dataBuf.Bytes(),
	}, nil
}

func (m *S3Manager) objectKey(blockID string, groupID uint32) string {
	return filepath.Join(m.prefix, blockID, fmt.Sprintf("%04x.cldts", groupID))
}
