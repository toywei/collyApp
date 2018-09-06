package mypack

import (
	"math/rand"
	"regexp"
	"fmt"
)

// Configuration | Colly http://go-colly.org/docs/introduction/configuration/
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandomString() string {
	b := make([]byte, rand.Intn(10)+10)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// 检查元素是否存在于数组？遍历？如何集合运算方法
func EleInArr(ele string, arr [] string) bool {
	for _, v := range arr {
		if ele == v {
			fmt.Println("eleInArr", ele)
			return true
		}
	}
	return false
}

// 检查href的是否为url
func IsUrl(str string) bool {
	reg := regexp.MustCompile("^https{0,1}:[A-Za-z0-9_\\-\\.\\/\\&\\?\\=]+$")
	data := reg.Find([]byte(str))
	if data == nil {
		return false
	}
	return true
}
