package eviction

import "github.com/tangrc99/MemTable/db/structure"

// Item 是数据库中存储的字段
type Item struct {
	Value any   // 真实值
	Evict int64 // 淘汰策略用值，越大越好
	structure.CostCounter
}

type Eviction interface {

	// KeyUsed 表示该键值被调用一次
	KeyUsed(key string, item *Item)

	KeyRemoved(key string)

	// Estimate 评估键值对的键值
	Estimate(key string) int64

	// Permitted 判断键是否通过准入门槛
	Permitted(key string) bool

	Clear()
}
