// Package structure 包含了 MemTable 数据库部分基础数据结构
package structure

import (
	"github.com/tangrc99/MemTable/logger"
	"github.com/tangrc99/MemTable/server/global"
	"hash/fnv"
	"math/rand"
	"regexp"
	"unsafe"
)

const MaxConSize = int(1<<31 - 1)
const shardBasicCost = int64(unsafe.Sizeof(Shard{}))
const dictBasicCost = int64(unsafe.Sizeof(Dict{}))

// Shard 是 Dict 中的一个分片
type Shard = map[string]Object

// Dict 包含了不同的分片，每一个分片包含一个哈希表
type Dict struct {
	shards []Shard // 存储键值对
	size   int     // table 分区数量
	count  int     // 键值对数量
	cost   int64   // 消耗的内存
}

// NewDict 创建指定分片数量的 Dict 并返回指针
func NewDict(size int) *Dict {
	if size <= 0 || size > MaxConSize {
		size = MaxConSize
	}
	dict := Dict{
		shards: make([]Shard, size),
		size:   size,
		count:  0,
		cost:   dictBasicCost + shardBasicCost*int64(size),
	}
	for i := 0; i < size; i++ {
		dict.shards[i] = make(map[string]Object)
	}
	return &dict
}

// hashKey 返回键值对应的分片号
func hashKey(key string) int {
	fnv32 := fnv.New32()
	key = "@#&" + key + "*^%$"
	_, _ = fnv32.Write([]byte(key))
	return int(fnv32.Sum32())
}

// countShard 返回键值对应的 *Shard
func (dict *Dict) countShard(key string) *Shard {
	pos := hashKey(key) % dict.size
	return &dict.shards[pos]
}

// Get 从 Dict 中查找键值对并返回值，如果不存在将会返回 nil
func (dict *Dict) Get(key string) (Object, bool) {

	shard := dict.countShard(key)
	obj, exist := (*shard)[key]
	return obj, exist
}

// Set 将键值对插入 Dict 对象中，该操作会覆盖原有键值对
func (dict *Dict) Set(key string, value Object) bool {

	shard := dict.countShard(key)

	if v, exist := (*shard)[key]; !exist {
		dict.count++
	} else {
		dict.cost -= v.Cost() + int64(len(key))
	}

	(*shard)[key] = value
	dict.cost += value.Cost() + int64(len(key))
	return true
}

// SetIfNotExist 将键值对插入 Dict 对象中，若键值对已存在将会返回 false
func (dict *Dict) SetIfNotExist(key string, value Object) bool {

	shard := dict.countShard(key)

	if _, exist := (*shard)[key]; exist {
		return false
	}

	(*shard)[key] = value
	dict.count++
	dict.cost += value.Cost() + int64(len(key))

	return true
}

// SetIfExist 覆盖原有的键值对，若键值对不存在将会返回 false
func (dict *Dict) SetIfExist(key string, value Object) bool {

	shard := dict.countShard(key)

	if v, exist := (*shard)[key]; exist {
		(*shard)[key] = value
		dict.cost -= v.Cost()
		dict.cost += value.Cost()
		return true
	}

	return false
}

// Update 覆盖原有的键值对，若键值对不存在将会返回 false
func (dict *Dict) Update(key string, value Object) bool {
	return dict.SetIfExist(key, value)
}

// Delete 删除指定键值对，成功删除返回 true，无元素返回 false
func (dict *Dict) Delete(key string) bool {
	shard := dict.countShard(key)

	if v, exist := (*shard)[key]; exist {
		delete(*shard, key)
		dict.count--
		dict.cost -= v.Cost() + int64(len(key))
		return true
	}

	return false
}

// DeleteGet 删除键值对并返回删除前的值，若键值对不存在则返回 nil
func (dict *Dict) DeleteGet(key string) Object {
	shard := dict.countShard(key)

	if value, exist := (*shard)[key]; exist {
		delete(*shard, key)
		dict.count--
		dict.cost -= value.Cost() + int64(len(key))

		return value
	}

	return nil
}

// Size 返回 Dict 中键值对数量
func (dict *Dict) Size() int {
	return dict.count
}

// Empty 用于判断 Dict 是否为空
func (dict *Dict) Empty() bool {
	return dict.count == 0
}

// Clear 删除 Dict 中的所有键值对
func (dict *Dict) Clear() {
	*dict = *NewDict(dict.size)
	dict.cost = dictBasicCost + shardBasicCost*int64(dict.size)
}

// Keys 返回匹配正则表达式全部键以及数量
func (dict *Dict) Keys(pattern string) ([]string, int) {
	keys := make([]string, dict.count)
	i := 0
	for _, shard := range dict.shards {
		for key := range shard {

			ok := true
			var err error
			if pattern != "" {
				ok, err = regexp.MatchString(pattern, key)
				if err != nil {
					logger.Error(err)
					continue
				}
			}
			if ok {
				keys[i] = key
				i++
			}
		}
	}

	return keys, i
}

// KeysByte 返回匹配正则表达式全部键以及数量，键值以[]byte形式返回
func (dict *Dict) KeysByte(pattern string) ([][]byte, int) {
	keys := make([][]byte, dict.count)
	i := 0
	for _, shard := range dict.shards {
		for key := range shard {

			ok := true
			var err error
			if pattern != "" {
				ok, err = regexp.MatchString(pattern, key)
				if err != nil {
					logger.Error(err)
					continue
				}
			}
			if ok {
				keys[i] = []byte(key)
				i++
			}
		}
	}

	return keys, i
}

// KeysWithTTL 返回全部未过期键，ttl 为记录过期时间的字典
func (dict *Dict) KeysWithTTL(ttl *Dict, pattern string) ([]string, int) {

	now := global.Now.Unix()

	keys := make([]string, dict.count)
	i := 0
	for _, shard := range dict.shards {

		for key := range shard {

			tp, exist := ttl.Get(key)
			if exist && tp.(Int64).Value() < now {
				// 如果过期需要删除
				v, _ := shard[key]
				dict.cost -= v.Cost() + int64(len(key))
				delete(shard, key)
				ttl.Delete(key)
			} else {

				ok := true
				var err error
				if pattern != "" {
					ok, err = regexp.MatchString(pattern, key)
					if err != nil {
						logger.Error(err)
						continue
					}
				}
				if ok {
					keys[i] = key
					i++
				}
			}
		}
	}

	return keys, i

}

// KeysWithTTLByte 返回全部未过期键，ttl 为记录过期时间的字典，键值以[]byte形式返回
func (dict *Dict) KeysWithTTLByte(ttl *Dict, pattern string) ([][]byte, int) {

	now := global.Now.Unix()

	keys := make([][]byte, dict.count)
	i := 0
	for _, shard := range dict.shards {

		for key := range shard {

			tp, exist := ttl.Get(key)
			if exist && tp.(Int64).Value() < now {
				// 如果过期需要删除
				v, _ := shard[key]
				dict.cost -= v.Cost() + int64(len(key))
				delete(shard, key)
				ttl.Delete(key)
			} else {

				ok := true
				var err error
				if pattern != "" {
					ok, err = regexp.MatchString(pattern, key)
					if err != nil {
						logger.Error(err)
						continue
					}
				}
				if ok {
					keys[i] = []byte(key)
					i++
				}
			}
		}
	}

	return keys, i

}

// Exist 判断键值对在 Dict 中是否存在
func (dict *Dict) Exist(key string) bool {

	shard := dict.countShard(key)
	_, exist := (*shard)[key]
	return exist
}

// Random 随机返回 Dict 中指定数量的键值对
func (dict *Dict) Random(num int) map[string]Object {

	selected := make(map[string]Object)

	// 这里优化为直接遍历
	if num >= dict.count {
		for _, shard := range dict.shards {
			for key, value := range shard {
				selected[key] = value
			}
		}
		return selected
	}

	for len(selected) < num {

		for i := 0; i < dict.size && len(selected) < num; i++ {
			for k, v := range dict.shards[i] {
				// 使用概率选择法，每一个 key 被选择的概率都是 1/n
				n := rand.Int() % dict.count
				if n == 0 {
					// 成功被选择
					selected[k] = v
				}
				if len(selected) < num {
					break
				}
			}
		}

	}

	return selected
}

// RandomKeys 随机返回 Dict 中指定数量的键，不返回值
func (dict *Dict) RandomKeys(num int) map[string]struct{} {
	selected := make(map[string]struct{})

	// 这里优化为直接遍历
	if num >= dict.count {
		for _, shard := range dict.shards {
			for key := range shard {
				selected[key] = struct{}{}
			}
		}
		return selected
	}

	for len(selected) < num {

		for i := 0; i < dict.size && len(selected) < num; i++ {
			for k := range dict.shards[i] {
				// 使用概率选择法，每一个 key 被选择的概率都是 1/n
				n := rand.Int() % dict.count
				if n == 0 {
					// 成功被选择
					selected[k] = struct{}{}
				}
				if len(selected) < num {
					break
				}
			}
		}

	}
	return selected
}

func (dict *Dict) GetAll() (*[]map[string]Object, int) {
	return &dict.shards, dict.count
}

// ShardCount 返回指定分片中的键值对数量
func (dict *Dict) ShardCount(shardSeq int) int {
	return len(dict.shards[shardSeq])
}

// KeysInShard 返回指定分片中的键值对
func (dict *Dict) KeysInShard(shardSeq, count int) ([]string, int) {
	keys := make([]string, count)
	i := 0
	for key := range dict.shards[shardSeq] {

		if i == count {
			break
		}

		keys[i] = key
		i++
	}
	return keys, i
}

func (dict *Dict) Cost() int64 {
	return dict.cost
}
