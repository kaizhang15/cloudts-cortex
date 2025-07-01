package tsobject

import (
	"context"
	"fmt"
	"io"
)

type ObjectWriter interface {
	WriteTo(w io.Writer) (int64, error)
}

type S3ObjectWriter struct {
	manager *S3Manager
	blockID string
}

func NewS3ObjectWriter(manager *S3Manager, blockID string) *S3ObjectWriter {
	return &S3ObjectWriter{
		manager: manager,
		blockID: blockID,
	}
}

func (w *S3ObjectWriter) WriteObject(ctx context.Context, obj ObjectWriter, groupID uint32) (string, error) {
	if tsobj, ok := obj.(*TSObject); ok {
		return w.manager.Upload(ctx, tsobj, w.blockID)
	}
	return "", fmt.Errorf("unsupported object type")
}
