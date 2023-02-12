package rand_str

import (
	"math/rand"
	"time"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// RandString 生成一个长度不小于 n 的随机字符串
func RandString(n int) string {
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[nR.Intn(len(letters))]
	}
	return string(b)
}

var hexLetters = []rune("0123456789abcdef")

// RandHexString 生成一个长度不小于 n 的随机十六进制字符串
func RandHexString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = hexLetters[rand.Intn(len(hexLetters))]
	}
	return string(b)
}
