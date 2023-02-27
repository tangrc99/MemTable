package eviction

import (
	"math"
)

type NoEviction struct {
}

func NewNoEviction() *NoEviction {
	return &NoEviction{}
}

// KeyUsed 表示该键值被调用一次
func (*NoEviction) KeyUsed(_ string, item *Item) {
}

// Estimate 评估键值对的键值
func (*NoEviction) Estimate(_ string) int64 {
	return math.MaxInt64
}

// Permitted 判断键是否通过准入门槛
func (*NoEviction) Permitted(_ string) bool {
	return true
}

func (*NoEviction) KeyRemoved(_ string) {
}

func (*NoEviction) Clear() {

}
