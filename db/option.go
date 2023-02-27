package db

import "github.com/tangrc99/MemTable/db/eviction"

type Option func(*DataBase)

type EvictPolicy int

const (
	LRU EvictPolicy = iota
	LFU
	NO
)

func WithEviction(policy EvictPolicy) Option {
	switch policy {
	case LRU:
		return func(db *DataBase) {
			db.evict = eviction.NewSampleLRU()
		}
	case LFU:
		return func(db *DataBase) {
			db.evict = eviction.NewTinyLFU(100)
		}
	}
	return func(*DataBase) {}
}

//func WithMemoryLimit(max uint64) Option {
//	return func(db *DataBase) {
//
//	}
//}
