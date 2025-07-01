// pkg/cloudts/cloudquerier.go
package cloudts

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/pb"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tagarray"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tagdict"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tsobject"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/ttmapping"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/protobuf/proto"
)

type CloudQuerier struct {
	s3Client     *s3.S3
	bucket       string
	tagDictCache *tagdict.TagDict
	logger       log.Logger
	cache        *queryCache
	closeChan    chan struct{}
	wg           sync.WaitGroup
}

type queryCache struct {
	ttMappings map[uint64]*ttmapping.TTMapping // partitionID -> TTMapping
	tagArrays  map[uint64]*tagarray.TagArray   // partitionID -> TagArray
	mu         sync.RWMutex
}

func newCloudQuerier(s3Config S3Config, logger log.Logger) *CloudQuerier {
	sess := newAWSSession(s3Config)
	q := &CloudQuerier{
		s3Client:     s3.New(sess),
		bucket:       s3Config.Bucket,
		logger:       logger,
		tagDictCache: loadLatestTagDict(s3.New(sess), s3Config.Bucket, logger),
		cache: &queryCache{
			ttMappings: make(map[uint64]*ttmapping.TTMapping),
			tagArrays:  make(map[uint64]*tagarray.TagArray),
		},
		closeChan: make(chan struct{}),
	}

	// 启动后台缓存清理协程
	q.wg.Add(1)
	go q.cleanupLoop()

	return q
}

func (q *CloudQuerier) Close() error {
	close(q.closeChan)
	q.wg.Wait()
	return nil
}

func (q *CloudQuerier) cleanupLoop() {
	defer q.wg.Done()

	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			q.cleanupCache()
		case <-q.closeChan:
			return
		}
	}
}

func (q *CloudQuerier) cleanupCache() {
	q.cache.mu.Lock()
	defer q.cache.mu.Unlock()

	// 清理1小时内未使用的缓存
	threshold := time.Now().Add(-time.Hour)
	for id, tm := range q.cache.ttMappings {
		if tm.LastUsed().Before(threshold) {
			delete(q.cache.ttMappings, id)
			delete(q.cache.tagArrays, id)
		}
	}
}

// QueryPartition 查询特定分区的数据
// func (q *CloudQuerier) QueryPartition(
// 	ctx context.Context,
// 	partitionID uint64,
// 	matchers []*labels.Matcher,
// ) ([]*tsobject.TSObject, error) {
// 	// 1. 加载分区元数据
// 	tm, ta, err := q.loadPartitionMetadata(ctx, partitionID)
// 	if err != nil {
// 		return nil, fmt.Errorf("load metadata failed: %w", err)
// 	}

// 	// 2. 解析查询条件
// 	tagEncodings := q.tagDictCache.ResolveMatchers(matchers)
// 	if len(tagEncodings) == 0 {
// 		return nil, nil
// 	}

// 	// 3. 获取匹配的时序组
// 	groupIDs := tm.GetGroupsByTags(tagEncodings)
// 	if len(groupIDs) == 0 {
// 		return nil, nil
// 	}

// 	// 4. 加载数据对象
// 	var objects []*tsobject.TSObject
// 	for _, gid := range groupIDs {
// 		obj, err := q.loadTSObject(ctx, partitionID, gid)
// 		if err != nil {
// 			return nil, fmt.Errorf("load TSObject failed: %w", err)
// 		}
// 		objects = append(objects, obj)
// 	}

// 	return objects, nil
// }

func (q *CloudQuerier) QueryPartition(
	ctx context.Context,
	partitionID uint64,
	matchers []*labels.Matcher,
) ([]*tsobject.TSObject, error) {
	tm, ta, err := q.loadPartitionMetadata(ctx, partitionID)
	if err != nil {
		return nil, fmt.Errorf("load metadata failed: %w", err)
	}

	// 使用TagArray优化查询
	filteredTags := q.filterTagsByFrequency(ta, matchers)
	if len(filteredTags) == 0 {
		return nil, nil
	}

	// 解析标签编码
	tagEncodings, err := q.tagDictCache.ResolveMatchers(filteredTags)
	if err != nil {
		return nil, fmt.Errorf("resolve matchers failed: %w", err)
	}

	// 获取匹配的时间序列
	seriesIDs := q.findMatchingSeries(tm, tagEncodings)
	if len(seriesIDs) == 0 {
		return nil, nil
	}

	// 转换为组ID并去重
	groupIDs := make(map[uint32]struct{})
	for _, id := range seriesIDs {
		groupIDs[uint32(id>>32)] = struct{}{} // 假设高32位是组ID
	}

	// 加载数据对象
	var objects []*tsobject.TSObject
	for gid := range groupIDs {
		obj, err := q.loadTSObject(ctx, partitionID, gid)
		if err != nil {
			return nil, fmt.Errorf("load TSObject failed: %w", err)
		}
		objects = append(objects, obj)
	}

	return objects, nil
}

// --- 私有方法 ---

func (q *CloudQuerier) loadPartitionMetadata(
	ctx context.Context,
	partitionID uint64,
) (*ttmapping.TTMapping, *tagarray.TagArray, error) {
	// 检查缓存
	q.cache.mu.RLock()
	tm, tmOk := q.cache.ttMappings[partitionID]
	ta, taOk := q.cache.tagArrays[partitionID]
	q.cache.mu.RUnlock()

	if tmOk && taOk {
		return tm, ta, nil
	}

	// 并行加载
	var (
		wg       sync.WaitGroup
		tmResult = make(chan *ttmapping.TTMapping, 1)
		taResult = make(chan *tagarray.TagArray, 1)
		errChan  = make(chan error, 2)
	)

	wg.Add(2)
	go func() {
		defer wg.Done()
		tm, err := q.loadTTMapping(ctx, partitionID)
		if err != nil {
			errChan <- err
			return
		}
		tmResult <- tm
	}()

	go func() {
		defer wg.Done()
		ta, err := q.loadTagArray(ctx, partitionID)
		if err != nil {
			errChan <- err
			return
		}
		taResult <- ta
	}()

	wg.Wait()
	close(tmResult)
	close(taResult)
	close(errChan)

	if len(errChan) > 0 {
		return nil, nil, <-errChan
	}

	tm, ta = <-tmResult, <-taResult

	// 更新缓存
	q.cache.mu.Lock()
	defer q.cache.mu.Unlock()
	q.cache.ttMappings[partitionID] = tm
	q.cache.tagArrays[partitionID] = ta

	return tm, ta, nil
}

func (q *CloudQuerier) loadTTMapping(ctx context.Context, partitionID uint64) (*ttmapping.TTMapping, error) {
	key := fmt.Sprintf("partitions/%d/ttmapping.bin", partitionID)
	data, err := q.downloadS3Object(ctx, key)
	if err != nil {
		return nil, err
	}

	var pbMap pb.TTMappingSnapshot
	if err := proto.Unmarshal(data, &pbMap); err != nil {
		return nil, err
	}

	return ttmapping.NewFromProto(&pbMap, q.tagDictCache), nil
}

func (q *CloudQuerier) loadTagArray(ctx context.Context, partitionID uint64) (*tagarray.TagArray, error) {
	key := fmt.Sprintf("partitions/%d/tagarray.bin", partitionID)
	data, err := q.downloadS3Object(ctx, key)
	if err != nil {
		return nil, err
	}

	var pbTA pb.TagArraySnapshot
	if err := proto.Unmarshal(data, &pbTA); err != nil {
		return nil, err
	}

	return tagarray.NewFromProto(&pbTA), nil
}

func (q *CloudQuerier) loadTSObject(ctx context.Context, partitionID uint64, groupID uint32) (*tsobject.TSObject, error) {
	key := fmt.Sprintf("partitions/%d/objects/%d.bin", partitionID, groupID)
	data, err := q.downloadS3Object(ctx, key)
	if err != nil {
		return nil, err
	}

	return tsobject.Deserialize(data)
}

func (q *CloudQuerier) downloadS3Object(ctx context.Context, key string) ([]byte, error) {
	resp, err := q.s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(q.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

// --- 初始化辅助函数 ---

func loadLatestTagDict(s3Client *s3.S3, bucket string, logger log.Logger) *tagdict.TagDict {
	// 列出所有TagDict版本
	resp, err := s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String("global/tagdict/"),
	})
	if err != nil {
		level.Error(logger).Log("msg", "list tagdict versions failed", "err", err)
		return tagdict.NewTagDict()
	}

	if len(resp.Contents) == 0 {
		return tagdict.NewTagDict()
	}

	// 找到最新版本
	var latestKey string
	var latestTime time.Time
	for _, obj := range resp.Contents {
		if obj.LastModified.After(latestTime) {
			latestKey = *obj.Key
			latestTime = *obj.LastModified
		}
	}

	// 下载最新版本
	data, err := downloadSingleObject(s3Client, bucket, latestKey)
	if err != nil {
		level.Error(logger).Log("msg", "download tagdict failed", "key", latestKey, "err", err)
		return tagdict.NewTagDict()
	}

	td := tagdict.NewTagDict()
	if err := td.Deserialize(data); err != nil {
		level.Error(logger).Log("msg", "deserialize tagdict failed", "err", err)
		return tagdict.NewTagDict()
	}
	return td
}

// 根据频率过滤标签
func (q *CloudQuerier) filterTagsByFrequency(ta *tagarray.TagArray, matchers []*labels.Matcher) []*labels.Matcher {
	if ta == nil || len(matchers) == 0 {
		return matchers
	}

	// 定义过滤策略
	const (
		highFreqThreshold = 0.8  // 高频标签阈值（覆盖80%以上时间序列）
		lowFreqThreshold  = 0.05 // 低频标签阈值（覆盖5%以下时间序列）
	)

	var filtered []*labels.Matcher
	totalSeries := ta.TotalSeries() // 需要在tagarray中实现这个方法

	for _, m := range matchers {
		// 只处理等值匹配器
		if m.Type != labels.MatchEqual {
			filtered = append(filtered, m)
			continue
		}

		// 获取标签编码
		tagPair := m.Name + "=" + m.Value
		enc, found := q.tagDictCache.GetEncoding(tagPair)
		if !found {
			level.Debug(q.logger).Log("msg", "tag not found in dictionary", "tag", tagPair)
			continue
		}

		// 查找标签频率
		freq := ta.GetFrequency(enc) // 需要在tagarray中实现这个方法
		coverage := float64(freq) / float64(totalSeries)

		// 过滤策略
		switch {
		case coverage >= highFreqThreshold:
			// 高频标签（如"region=us-east-1"），过滤掉因为选择性太低
			level.Debug(q.logger).Log("msg", "skipping high frequency tag",
				"tag", tagPair, "coverage", coverage)
			continue
		case coverage <= lowFreqThreshold:
			// 低频标签（如"pod=xyz123"），可能太具体
			level.Debug(q.logger).Log("msg", "skipping low frequency tag",
				"tag", tagPair, "coverage", coverage)
			continue
		default:
			// 中等频率标签，保留
			filtered = append(filtered, m)
		}
	}

	// 如果没有过滤出任何标签，返回原始匹配器避免空结果
	if len(filtered) == 0 {
		level.Debug(q.logger).Log("msg", "all tags filtered, using original matchers")
		return matchers
	}

	return filtered
}

func downloadSingleObject(s3Client *s3.S3, bucket, key string) ([]byte, error) {
	resp, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// 查找匹配的时间序列
func (q *CloudQuerier) findMatchingSeries(tm *ttmapping.TTMapping, tagEncodings []uint32) []uint64 {
	if len(tagEncodings) == 0 {
		return nil
	}

	// 获取第一个标签的匹配序列
	series := tm.GetSeriesByTag(tagEncodings[0])
	if len(tagEncodings) == 1 {
		return series
	}

	// 多个标签需要求交集
	result := make([]uint64, 0, len(series))
	for _, id := range series {
		matched := true
		for _, enc := range tagEncodings[1:] {
			if !tm.HasSeriesTag(id, enc) {
				matched = false
				break
			}
		}
		if matched {
			result = append(result, id)
		}
	}
	return result
}
