// package tagdict

// import (
// 	"os"
//     "testing"
//     // 使用完整模块路径导入
//     // "github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tagdict"
// )

// func TestTagDict(t *testing.T) {
// 	td := NewTagDict()

// 	// 测试metric插入和查询
// 	metricEnc := td.Insert("cpu_usage")
// 	if str, ok := td.GetTagString(metricEnc); !ok || str != "cpu_usage" {
// 		t.Errorf("metric lookup failed, got: %v, want: cpu_usage", str)
// 	}

// 	// 测试tag插入和查询
// 	tagEnc := td.Insert("instance=node01")
// 	if str, ok := td.GetTagString(tagEnc); !ok || str != "instance=node01" {
// 		t.Errorf("tag lookup failed, got: %v, want: instance=node01", str)
// 	}

// 	// 测试不存在的编码
// 	if _, ok := td.GetTagString(999); ok {
// 		t.Error("unexpected success for invalid encoding")
// 	}
// }
// func TestTagDictPersistence(t *testing.T) {
// 	// 1. 初始化并插入数据
// 	td := NewTagDict()
// 	enc1 := td.Insert("cpu=core1")
// 	enc2 := td.Insert("memory_usage")

// 	// 2. 保存快照
// 	tmpFile := "/tmp/test_snapshot.bin"
// 	if err := td.SaveSnapshotLocal(tmpFile); err != nil {
// 		t.Fatalf("SaveSnapshotLocal failed: %v", err)
// 	}
// 	defer os.Remove(tmpFile) // 测试完成后清理

// 	// 3. 从快照恢复
// 	data, err := os.ReadFile(tmpFile)
// 	if err != nil {
// 		t.Fatalf("ReadFile failed: %v", err)
// 	}

// 	restored, err := LoadTagDictFromSnapshot(data)
// 	if err != nil {
// 		t.Fatalf("LoadTagDictFromSnapshot failed: %v", err)
// 	}

// 	// 4. 验证数据
// 	tests := []struct {
// 		enc  uint32
// 		want string
// 	}{
// 		{enc1, "cpu=core1"},
// 		{enc2, "memory_usage"},
// 	}

//		for _, tt := range tests {
//			if str, ok := restored.GetTagString(tt.enc); !ok || str != tt.want {
//				t.Errorf("GetTagString(%d) = (%v, %v), want (%s, true)", tt.enc, str, ok, tt.want)
//			}
//		}
//	}
package tagdict

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 辅助函数：批量插入标签
func insertTestTags(td *TagDict, tags []string) {
	for _, tag := range tags {
		td.Insert(tag)
	}
}

func TestTagDict_Serialization(t *testing.T) {
	// 1. 创建测试数据
	td := NewTagDict()
	tags := []string{
		"cpu=core1",      // 标签
		"region=us-east", // 标签
		"http_requests",  // 指标
	}
	insertTestTags(td, tags)

	// 2. 序列化
	data, err := td.Serialize()
	assert.NoError(t, err)
	assert.NotEmpty(t, data)

	// 3. 反序列化
	newTD := NewTagDict()
	err = newTD.Deserialize(data)
	assert.NoError(t, err)

	// 4. 验证基础属性
	assert.Equal(t, td.nextEncoding, newTD.nextEncoding)

	// 5. 验证标签编码
	for _, tag := range tags {
		origEnc := getEncoding(td, tag) // 辅助函数避免直接访问内部字段
		newEnc := getEncoding(newTD, tag)
		assert.Equal(t, origEnc, newEnc)
	}
}

// 辅助函数：安全获取编码
func getEncoding(td *TagDict, tag string) uint32 {
	if strings.Contains(tag, "=") {
		td.reverseIndex.lock.RLock()
		defer td.reverseIndex.lock.RUnlock()
		for enc, t := range td.reverseIndex.tags {
			if t == tag {
				return enc
			}
		}
	} else {
		td.reverseIndex.lock.RLock()
		defer td.reverseIndex.lock.RUnlock()
		for enc, m := range td.reverseIndex.metrics {
			if m == tag {
				return enc
			}
		}
	}
	return 0
}

func TestTagDict_Empty(t *testing.T) {
	td := NewTagDict()

	// 序列化空字典
	data, err := td.Serialize()
	assert.NoError(t, err)

	// 反序列化
	newTD := NewTagDict()
	err = newTD.Deserialize(data)
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), newTD.nextEncoding)
}

func TestTagDict_Insert(t *testing.T) {
	td := NewTagDict()

	testCases := []struct {
		name     string
		tag      string
		isMetric bool
	}{
		{"TagPair0", "cpu=core2", false},
		{"TagPair1", "cpu=core1", false},
		{"Metric", "http_requests", true},
		{"SpecialChars", "label.with.dots=value@special", false},
	}

	// 记录已分配的编码用于验证唯一性
	seenEncodings := make(map[uint32]string)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			enc := td.Insert(tc.tag)

			// 验证1: 相同标签返回相同编码
			enc2 := td.Insert(tc.tag)
			assert.Equal(t, enc, enc2, "same tag should return same encoding")

			// 验证2: 编码映射正确性
			// var found bool
			if tc.isMetric {
				td.reverseIndex.lock.RLock()
				storedTag, found := td.reverseIndex.metrics[enc]
				td.reverseIndex.lock.RUnlock()
				assert.True(t, found, "metric encoding not found in reverse index")
				assert.Equal(t, tc.tag, storedTag, "stored metric tag mismatch")
			} else {
				td.reverseIndex.lock.RLock()
				storedTag, found := td.reverseIndex.tags[enc]
				td.reverseIndex.lock.RUnlock()
				assert.True(t, found, "tag encoding not found in reverse index")
				assert.Equal(t, tc.tag, storedTag, "stored tag mismatch")
			}

			// 验证3: 编码唯一性(除非是重复标签)
			if existingTag, exists := seenEncodings[enc]; exists {
				assert.Equal(t, tc.tag, existingTag, "encoding collision between different tags")
			} else {
				seenEncodings[enc] = tc.tag
			}
		})
	}
}

func TestTagDict_Deserialize_InvalidData(t *testing.T) {
	td := NewTagDict()

	tests := []struct {
		name        string
		data        []byte
		expectError bool
	}{
		{"NilData", nil, true},
		{"InvalidProto", []byte("invalid data"), true},
		{"EmptyData", []byte{}, true},
		{"ValidData", []byte{0x0a, 0x00}, false}, // 最小有效protobuf
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := td.Deserialize(tt.data)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
