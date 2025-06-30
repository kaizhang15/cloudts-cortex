package ttmapping

import (
	"testing"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tagdict"
)

func TestResolveTagEncodings(t *testing.T) {
	td := tagdict.NewTagDict()
	
	// 准备测试数据
	enc1 := td.Insert("job=nginx")
	enc2 := td.Insert("job=mysql")
	// enc3 := td.Insert("instance=10.0.0.1")

	tests := []struct {
		name     string
		matchers []*labels.Matcher
		expected []uint32
	}{
		{
			name: "equal match",
			matchers: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: "job", Value: "nginx"},
			},
			expected: []uint32{enc1},
		},
		{
			name: "regex match",
			matchers: []*labels.Matcher{
				{Type: labels.MatchRegexp, Name: "job", Value: "my.*"},
			},
			expected: []uint32{enc2},
		},
		{
			name: "not equal",
			matchers: []*labels.Matcher{
				{Type: labels.MatchNotEqual, Name: "job", Value: "nginx"},
			},
			expected: []uint32{enc2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encs := resolveTagEncodings(tt.matchers, td)
			assert.ElementsMatch(t, tt.expected, encs)
		})
	}
}