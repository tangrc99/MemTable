package db

import "github.com/tangrc99/MemTable/db/eviction"

type Option func(*DataBase)

type EvictPolicy int

const (
	EvictLRU EvictPolicy = iota
	EvictLFU
	NoEviction
)

func WithEviction(policy EvictPolicy) Option {
	switch policy {
	case EvictLRU:
		return func(db *DataBase) {
			db.enableEvict = true
			db.evict = eviction.NewSampleLRU()
		}
	case EvictLFU:
		return func(db *DataBase) {
			db.enableEvict = true
			db.evict = eviction.NewTinyLFU(100)
		}
	}

	return func(db *DataBase) {
		db.enableEvict = false
		db.evict = eviction.NewNoEviction()
	}
}

func WithRookies() Option {
	return func(db *DataBase) {
		db.rookies = eviction.NewRookieList()
	}
}

// WithEvictNotification 当数据库因为过期，或者内存已满发生淘汰时，会向 channel 发送一个通知
func WithEvictNotification(evictNotification chan string) Option {
	return func(db *DataBase) {
		db.notifies = evictNotification
		db.enableNotification = true
	}
}

//func WithMemoryLimit(max uint64) Option {
//	return func(db *DataBase) {
//
//	}
//}
