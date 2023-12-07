package util

import (
	"encoding/json"
	"fmt"
)

// PrintJson 输出格式化后的json字符串
func PrintJson(info interface{}) {
	jsonStr, _ := json.MarshalIndent(info, "", "    ")
	fmt.Println(string(jsonStr))
}
