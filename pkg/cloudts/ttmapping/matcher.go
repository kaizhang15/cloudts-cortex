package ttmapping

import (
	"strings"
	"regexp"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tagdict"
)

// resolveTagEncodings 将标签匹配器转换为标签编码列表
func resolveTagEncodings(matchers []*labels.Matcher, td *tagdict.TagDict) []uint32 {
	var encodings []uint32

	for _, matcher := range matchers {
		switch matcher.Type {
		case labels.MatchEqual:      // =
			if enc, ok := td.GetEncoding(matcher.Name + "=" + matcher.Value); ok {
				encodings = append(encodings, enc)
			}
		case labels.MatchNotEqual:  // !=
			encodings = append(encodings, getNotEqualEncodings(matcher, td)...)
		case labels.MatchRegexp:    // =~
			encodings = append(encodings, getRegexpEncodings(matcher, td)...)
		case labels.MatchNotRegexp: // !~
			encodings = append(encodings, getNotRegexpEncodings(matcher, td)...)
		}
	}

	return deduplicateEncodings(encodings)
}

// 处理 != 操作符
func getNotEqualEncodings(matcher *labels.Matcher, td *tagdict.TagDict) []uint32 {
	var encodings []uint32
	td.IterateEncodings(func(enc uint32, tagPair string) bool {
		name, value := splitTagPair(tagPair)
		if name == matcher.Name && value != matcher.Value {
			encodings = append(encodings, enc)
		}
		return true
	})
	return encodings
}

// 处理 =~ 正则匹配
func getRegexpEncodings(matcher *labels.Matcher, td *tagdict.TagDict) []uint32 {
	var encodings []uint32
	re, _ := regexp.Compile(matcher.Value)
	// if err != nil {
	// 	return false
	// }
	td.IterateEncodings(func(enc uint32, tagPair string) bool {
		name, value := splitTagPair(tagPair)
		if name == matcher.Name && re.MatchString(value) {
			encodings = append(encodings, enc)
		}
		return true
	})
	return encodings
}

// 处理 !~ 正则非匹配
func getNotRegexpEncodings(matcher *labels.Matcher, td *tagdict.TagDict) []uint32 {
	var encodings []uint32
	re, _ := regexp.Compile(matcher.Value)
	// if err != nil {
	// 	return false
	// }
	td.IterateEncodings(func(enc uint32, tagPair string) bool {
		name, value := splitTagPair(tagPair)
		if name == matcher.Name && !re.MatchString(value) {
			encodings = append(encodings, enc)
		}
		return true
	})
	return encodings
}

// --- 工具函数 ---

// 拆分 "name=value" 格式的标签对
func splitTagPair(tagPair string) (name, value string) {
	parts := strings.SplitN(tagPair, "=", 2)
	if len(parts) == 1 {
		return parts[0], "" // 处理纯metric情况
	}
	return parts[0], parts[1]
}

// 去重编码列表
func deduplicateEncodings(encs []uint32) []uint32 {
	seen := make(map[uint32]bool)
	var result []uint32
	for _, enc := range encs {
		if !seen[enc] {
			seen[enc] = true
			result = append(result, enc)
		}
	}
	return result
}