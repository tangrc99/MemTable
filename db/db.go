// Package db 包含了 MemTable 中数据库的主要数据结构和算法
package db

import (
	"github.com/tangrc99/MemTable/db/eviction"
	"github.com/tangrc99/MemTable/db/structure"
	"math"
	"time"
)

// DataBase 代表一个内存数据库，包含键值对，ttl，watch等信息。同一个 DataBase 实例中键值不能重复，
// 不同的实例键值可以重复。
type DataBase struct {
	dict    *structure.Dict // 存储键值对
	ttlKeys *structure.Dict // 存储过期键
	watches *structure.Dict // 存储监视键
	evict   eviction.Eviction
}

// NewDataBase 创建一个新 DataBase 实例，并返回指针
func NewDataBase(slot int, ops ...Option) *DataBase {
	db := &DataBase{
		dict:    structure.NewDict(slot),
		ttlKeys: structure.NewDict(1),
		watches: structure.NewDict(1),
		evict:   eviction.NewNoEviction(),
	}
	for _, op := range ops {
		op(db)
	}
	return db
}

// checkNotExpired 检查键是否过期，如果过期则会自动删除键值对并返回 false
func (db_ *DataBase) checkNotExpired(key string) bool {

	ttl, exist := db_.ttlKeys.Get(key)
	if !exist {
		return true
	}

	if ttl.(structure.Int64).Value() > time.Now().Unix() {
		// 如果没有过期
		return true
	}

	db_.dict.Delete(key)
	db_.ttlKeys.Delete(key)
	return false
}

// RemoveTTL 删除键的 TTL 信息，如果 TTL 则返回 false
func (db_ *DataBase) RemoveTTL(key string) bool {
	return db_.ttlKeys.Delete(key)
}

// GetTTL 得到一个键的 TTL 信息，如果 TTL 存在会返回一个 timestamp；如果 TTL 不存在则会返回-1；
// 如果 TTL 已经过期则会删除 TTL 信息并返回-2
func (db_ *DataBase) GetTTL(key string) int64 {

	ttl, exist := db_.ttlKeys.Get(key)
	if exist {
		// 如果存在 ttl，检查过期时间
		now := time.Now().Unix()
		r := ttl.(structure.Int64).Value() - now
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

// GetKey 查询数据库中是否存在该键值，如果键值存在且为过期，返回键对应的值；若键已经过期，将会删除该键值对，并返回 nil
func (db_ *DataBase) GetKey(key string) (any, bool) {
	ok := db_.checkNotExpired(key)
	if !ok {
		return nil, false
	}
	item, exist := db_.dict.Get(key)
	if exist {
		db_.evict.KeyUsed(key, item.(*eviction.Item))
		return item.(*eviction.Item).Value, true
	}
	return nil, false
}

// SetKey 将键值对插入到 DataBase 中，该操作可能会覆盖旧键。
func (db_ *DataBase) SetKey(key string, value any) bool {
	item := &eviction.Item{Value: value}
	db_.dict.Set(key, item)
	db_.evict.KeyUsed(key, item)
	return true
}

// SetTTL 设置键值对的 TTL 信息，ttl 为 unix 时间戳。若键值对不存在，将会返回 false
func (db_ *DataBase) SetTTL(key string, ttl int64) bool {
	if !db_.dict.Exist(key) {
		return false
	}
	db_.ttlKeys.Set(key, structure.Int64(ttl))
	return true
}

// SetKeyWithTTL 将键值对插入到 DataBase 中，并设置 TTL 信息，该操作可能会覆盖旧键。
func (db_ *DataBase) SetKeyWithTTL(key string, value any, ttl int64) bool {
	item := &eviction.Item{Value: value}
	db_.dict.Set(key, item)
	db_.ttlKeys.Set(key, structure.Int64(ttl))
	db_.evict.KeyUsed(key, item)
	return true
}

// DeleteKey 将会删除 DataBase 中对应的键值对，若键不存在，返回 false
func (db_ *DataBase) DeleteKey(key string) bool {

	db_.ttlKeys.Delete(key)

	return db_.dict.Delete(key)
}

// RenameKey 将键值对的键重命名，同时转移 TTL 信息，该操作可能会覆盖旧键值对
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

// ExistKey 用于判断键是否存在
func (db_ *DataBase) ExistKey(key string) bool {

	ok := db_.checkNotExpired(key)
	if !ok {
		return false
	}

	return db_.dict.Exist(key)
}

// Keys 返回 DataBase 中通过正则表达式匹配的所有键
func (db_ *DataBase) Keys(pattern string) (keys []string, nums int) {
	return db_.dict.KeysWithTTL(db_.ttlKeys, pattern)
}

// KeysByte 返回 DataBase 中通过正则表达式匹配的所有键，键以 []byte 类型存储
func (db_ *DataBase) KeysByte(pattern string) (keys [][]byte, nums int) {
	return db_.dict.KeysWithTTLByte(db_.ttlKeys, pattern)
}

// RandomKey 随机返回一个键，如果 DataBase 不存在键值对，将会返回空字符串
func (db_ *DataBase) RandomKey() (string, bool) {
	keys := db_.dict.Random(1)
	for k := range keys {
		return k, true
	}
	return "", false
}

// CleanExpiredKeys 在 db 中随机抽取 samples 个数的 ttl key，如果过期则删除，并返回删除掉的个数
func (db_ *DataBase) CleanExpiredKeys(samples int) int {

	now := time.Now().Unix()

	ttls := db_.ttlKeys.Random(samples)
	deleted := 0
	for key, expire := range ttls {
		if expire.(structure.Int64).Value() < now {
			deleted++
			db_.ttlKeys.Delete(key)
			db_.dict.Delete(key)
		}
	}
	return deleted
}

// Clear 用于情况 DataBase 中的所有信息
func (db_ *DataBase) Clear() {
	db_.dict = structure.NewDict(12)
	db_.ttlKeys = structure.NewDict(1)
}

// Size 返回数据库中键值对数量，函数不会检查键值对的过期情况。
func (db_ *DataBase) Size() int {
	return db_.dict.Size()
}

// TTLSize 返回数据库中具有 TTL 信息的键值对数量，函数不会检查键值对的过期情况。
func (db_ *DataBase) TTLSize() int {
	return db_.ttlKeys.Size()
}

// Watch 监控一个键是否被修改，如果键值被修改 flag 变量将会被设置为 false
func (db_ *DataBase) Watch(key string, flag *bool) {

	v, ok := db_.watches.Get(key)
	if !ok {
		flags := make(map[*bool]struct{})
		flags[flag] = struct{}{}
		db_.watches.Set(key, &flags)
		return
	}

	flags := v.(*map[*bool]struct{})
	(*flags)[flag] = struct{}{}
}

// UnWatch 取消对键的监控
func (db_ *DataBase) UnWatch(key string, flag *bool) {
	v, ok := db_.watches.Get(key)
	if !ok {
		return
	}
	flags := v.(*map[*bool]struct{})
	delete(*flags, flag)

	if len(*flags) == 0 {
		db_.watches.Delete(key)
	}
}

// ReviseNotify 通知键修改
func (db_ *DataBase) ReviseNotify(key string) {
	v, ok := db_.watches.Get(key)
	if !ok {
		return
	}
	flags := v.(*map[*bool]struct{})

	for flag := range *flags {
		*flag = true
	}
}

// ReviseNotifyAll 通知所有被 watch 的键修改，用于 flushdb 和 flushall 命令
func (db_ *DataBase) ReviseNotifyAll() {

	dicts, _ := db_.watches.GetAll()

	for _, dict := range *dicts {
		for _, v := range dict {
			flags := v.(*map[*bool]struct{})
			for flag := range *flags {
				*flag = true
			}
		}
	}
}

// WatchSize 返回数据库中被监控的键值对数目
func (db_ *DataBase) WatchSize() int {
	return db_.watches.Size()
}

func (db_ *DataBase) SlotCount(slotSeq int) int {
	return db_.dict.ShardCount(slotSeq)
}

func (db_ *DataBase) KeysInSlot(slotSeq, count int) ([]string, int) {
	return db_.dict.KeysInShard(slotSeq, count)
}

func (db_ *DataBase) IsKeyPermitted(key string) int64 {
	if !db_.evict.Permitted(key) {
		return -1
	}
	return db_.evict.Estimate(key)
}

func (db_ *DataBase) EvictKeys(access, roomNeeded int64) (evicted []string, accepted bool) {

	victims := make([]string, 0, roomNeeded)

	for room := int64(0); room < roomNeeded; {
		var minKey string
		var minEvict = int64(math.MaxInt64)
		for k, v := range db_.dict.Random(10) {
			if v.(*eviction.Item).Evict < minEvict {
				minKey = k
			}
		}

		// 不满足驱逐条件
		if db_.evict.Estimate(minKey) > access {
			return victims, false
		}

		victims = append(victims, minKey)
		room++
	}
	return victims, true
}
