package eviction

import (
	"github.com/tangrc99/MemTable/db/structure"
)

// Item 是数据库中存储的字段
type Item struct {
	Value structure.Object // 真实值
	Evict int64            // 淘汰策略用值，越大越好
}

func (item *Item) Cost() int64 {
	return 8 + item.Value.Cost()
}

// Eviction 是对数据库中驱逐元素算法的抽象
type Eviction interface {

	// KeyUsed 表示该键值被调用一次
	KeyUsed(key string, item *Item)

	KeyRemoved(key string)

	// Estimate 评估键值对的键值，更有键值的键应该返回更大的值
	Estimate(key string) int64

	// Permitted 判断键是否通过准入门槛
	Permitted(key string) bool

	Clear()
}
