package db

import (
	"MemTable/db/structure"
	"time"
)

type DataBase struct {
	dict    *structure.Dict // 存储键值对
	ttlKeys *structure.Dict // 存储过期键
}

// NewDataBase Create a new database impl
func NewDataBase() *DataBase {
	return &DataBase{
		dict:    structure.NewDict(12),
		ttlKeys: structure.NewDict(1),
	}
}

func (db_ *DataBase) CheckAndRemoveIfExpired(key string) bool {

	ttl, exist := db_.ttlKeys.Get(key)
	if !exist {
		return true
	}

	if ttl.(int64) > time.Now().Unix() {
		return true
	}

	db_.dict.Delete(key)
	db_.ttlKeys.Delete(key)
	return false
}

func (db_ *DataBase) RemoveTTL(key string) bool {
	return db_.ttlKeys.Delete(key)
}

func (db_ *DataBase) GetKey(key string) (any, bool) {
	ok := db_.CheckAndRemoveIfExpired(key)
	if !ok {
		return nil, false
	}
	return db_.dict.Get(key)
}

func (db_ *DataBase) SetKey(key string, value any) bool {

	db_.dict.Set(key, value)
	return true
}

func (db_ *DataBase) SetTTL(key string, ttl int64) bool {
	if !db_.dict.Exist(key) {
		return false
	}
	db_.ttlKeys.Set(key, ttl)
	return true
}

func (db_ *DataBase) SetKeyWithTTL(key string, value any, ttl int64) bool {

	db_.dict.Set(key, value)
	db_.ttlKeys.Set(key, ttl)

	return true
}
