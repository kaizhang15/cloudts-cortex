// pkg/cloudts/tsobject/encoder.go

package tsobject

import (
    "github.com/prometheus/prometheus/tsdb/chunkenc"
    "github.com/cespare/xxhash"
    "github.com/aws/aws-sdk-go/service/s3"
    "sync"
)

type TSObject struct {
    TimePartition uint64
    GroupID       uint32
    Chunks        []*DataChunk
}

type DataChunk struct {
    TimeseriesID uint64
    Timestamps   []int64     // 使用 Delta-of-Delta 压缩
    Values       []float64   // 使用 Gorilla 压缩
}

type CloudWriter struct {
    S3Client *s3.S3
}

// 上传数据块到 S3
func (cw *CloudWriter) Upload(block *DataBlock) error {
    tsObject := &TSObject{
        TimePartition: block.TimePartition,
        GroupID:      cw.calculateGroupID(block.Tags),
        Chunks:       cw.compressChunks(block.RawChunks),
    }
    key := fmt.Sprintf("ts/%d/%d", tsObject.TimePartition, tsObject.GroupID)
    _, err := cw.S3Client.PutObject(&s3.PutObjectInput{
        Bucket: aws.String("my-bucket"),
        Key:    aws.String(key),
        Body:   serializeTSObject(tsObject),
    })
    return err
}

// 并行查询多个 TSObject
func (cq *CloudQuerier) QueryParallel(tagEncodings []uint32, timeRange TimeRange) []DataPoint {
    // 1. 从 TTMapping 获取时间序列 ID 和分组
    timeseriesIDs := cq.Mapping.GetTimeseriesByTags(tagEncodings)
    groupIDs := cq.GroupTimeseries(timeseriesIDs)
    
    // 2. 并行下载 TSObject
    var wg sync.WaitGroup
    results := make(chan []DataPoint, len(groupIDs))
    for _, gid := range groupIDs {
        wg.Add(1)
        go func(groupID uint32) {
            defer wg.Done()
            tsObject := cq.downloadTSObject(groupID)
            results <- tsObject.Filter(timeRange)
        }(gid)
    }
    wg.Wait()
    close(results)
    
    // 3. 合并结果
    var merged []DataPoint
    for points := range results {
        merged = append(merged, points...)
    }
    return merged
}

