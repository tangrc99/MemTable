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

// CheckNotExpired 没有过期返回 true
func (db_ *DataBase) CheckNotExpired(key string) bool {

	ttl, exist := db_.ttlKeys.Get(key)
	if !exist {
		return true
	}

	if ttl.(int64) > time.Now().Unix() {
		// 如果没有过期
		return true
	}

	db_.dict.Delete(key)
	db_.ttlKeys.Delete(key)
	return false
}

func (db_ *DataBase) RemoveTTL(key string) bool {
	return db_.ttlKeys.Delete(key)
}

func (db_ *DataBase) GetTTL(key string) int64 {

	ttl, exist := db_.ttlKeys.Get(key)
	if exist {
		// 如果存在 ttl，检查过期时间
		now := time.Now().Unix()
		r := ttl.(int64) - now
		if r < 0 {
			db_.ttlKeys.Delete(key)
			db_.dict.Delete(key)
			return -2
		}
		return r
	}

	_, exist = db_.dict.Get(key)
	if !exist {
		return -2
	}

	return -1
}

func (db_ *DataBase) GetKey(key string) (any, bool) {
	ok := db_.CheckNotExpired(key)
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

func (db_ *DataBase) DeleteKey(key string) bool {

	db_.ttlKeys.Delete(key)

	return db_.dict.Delete(key)
}

func (db_ *DataBase) RenameKey(old, new string) bool {

	// 顺带检查 ttl 是否过期
	value, ok := db_.GetKey(old)
	if !ok {
		return false
	}

	ttl, ok := db_.ttlKeys.Get(old)
	db_.ttlKeys.Delete(old)
	db_.dict.Delete(old)

	db_.dict.Set(new, value)
	db_.ttlKeys.Set(new, ttl)
	return true
}

func (db_ *DataBase) ExistKey(key string) bool {

	ok := db_.CheckNotExpired(key)
	if !ok {
		return false
	}

	return db_.dict.Exist(key)
}

func (db_ *DataBase) Keys(pattern string) ([]string, int) {
	return db_.dict.KeysWithTTL(db_.ttlKeys, pattern)
}

func (db_ *DataBase) KeysByte(pattern string) ([][]byte, int) {
	return db_.dict.KeysWithTTLByte(db_.ttlKeys, pattern)
}

func (db_ *DataBase) RandomKey() (string, bool) {
	keys := db_.dict.Random(1)
	for k := range keys {
		return k, true
	}
	return "", false
}

// CleanTTLKeys 在 db 中随机抽取 samples 个数的 ttl key，如果过期则删除，并返回删除掉的个数
func (db_ *DataBase) CleanTTLKeys(samples int) int {

	now := time.Now().Unix()

	ttls := db_.ttlKeys.Random(samples)
	deleted := 0
	for key, expire := range ttls {
		if expire.(int64) < now {
			deleted++
			db_.ttlKeys.Delete(key)
			db_.dict.Delete(key)
		}
	}
	return deleted
}

func (db_ *DataBase) Clear() {
	db_.dict = structure.NewDict(12)
	db_.ttlKeys = structure.NewDict(1)
}

func (db_ *DataBase) Size() int {
	return db_.dict.Size()
}
