package eviction

import (
	"math"
	"time"
)

type SampleLRU struct {
	Eviction
}

func NewSampleLRU() *SampleLRU {
	return &SampleLRU{}
}

// KeyUsed 表示该键值被调用一次
func (*SampleLRU) KeyUsed(_ string, item *Item) {
	item.Evict = time.Now().Unix()
}

// Estimate 评估键值对的键值
func (*SampleLRU) Estimate(_ string) int64 {
	return math.MaxInt64
}

// Permitted 判断键是否通过准入门槛
func (*SampleLRU) Permitted(_ string) bool {
	return true
}
