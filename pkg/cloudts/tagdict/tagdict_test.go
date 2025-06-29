package tagdict

import (
	"os"
    "testing"
    // 使用完整模块路径导入
    // "github.com/kaizhang15/cortex-cloudts/pkg/cloudts/tagdict" 
)

func TestTagDict(t *testing.T) {
	td := NewTagDict()

	// 测试metric插入和查询
	metricEnc := td.Insert("cpu_usage")
	if str, ok := td.GetTagString(metricEnc); !ok || str != "cpu_usage" {
		t.Errorf("metric lookup failed, got: %v, want: cpu_usage", str)
	}

	// 测试tag插入和查询
	tagEnc := td.Insert("instance=node01")
	if str, ok := td.GetTagString(tagEnc); !ok || str != "instance=node01" {
		t.Errorf("tag lookup failed, got: %v, want: instance=node01", str)
	}

	// 测试不存在的编码
	if _, ok := td.GetTagString(999); ok {
		t.Error("unexpected success for invalid encoding")
	}
}
func TestTagDictPersistence(t *testing.T) {
	// 1. 初始化并插入数据
	td := NewTagDict()
	enc1 := td.Insert("cpu=core1")
	enc2 := td.Insert("memory_usage")

	// 2. 保存快照
	tmpFile := "/tmp/test_snapshot.bin"
	if err := td.SaveSnapshotLocal(tmpFile); err != nil {
		t.Fatalf("SaveSnapshotLocal failed: %v", err)
	}
	defer os.Remove(tmpFile) // 测试完成后清理

	// 3. 从快照恢复
	data, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	restored, err := LoadTagDictFromSnapshot(data)
	if err != nil {
		t.Fatalf("LoadTagDictFromSnapshot failed: %v", err)
	}

	// 4. 验证数据
	tests := []struct {
		enc  uint32
		want string
	}{
		{enc1, "cpu=core1"},
		{enc2, "memory_usage"},
	}

	for _, tt := range tests {
		if str, ok := restored.GetTagString(tt.enc); !ok || str != tt.want {
			t.Errorf("GetTagString(%d) = (%v, %v), want (%s, true)", tt.enc, str, ok, tt.want)
		}
	}
}