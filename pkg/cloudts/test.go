package cloudts

import (
	"fmt"

	"github.com/kaizhang15/cloudts-cortex/pkg/cloudts/tagdict"
)

func main() {
	td := tagdict.NewTagDict()

	// 插入数据
	enc1 := td.Insert("http_requests")
	enc2 := td.Insert("instance=10.0.0.1")

	// 查询数据
	str1, _ := td.GetTagString(enc1)
	str2, _ := td.GetTagString(enc2)

	fmt.Println(str1) // 输出: http_requests
	fmt.Println(str2) // 输出: instance=10.0.0.1
}
