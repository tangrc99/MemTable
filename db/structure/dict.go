package structure

import (
	"hash/fnv"
	"math/rand"
)

const MaxConSize = int(1<<31 - 1)

type shard = map[string]any

type Dict struct {
	shards []shard // 存储键值对
	size   int     // table 分区数量
	count  int64   // 键值对数量
}

func NewDict(size int) *Dict {
	if size <= 0 || size > MaxConSize {
		size = MaxConSize
	}
	dict := Dict{
		shards: make([]shard, size),
		size:   size,
		count:  0,
	}
	for i := 0; i < size; i++ {
		dict.shards[i] = make(map[string]any)
	}
	return &dict
}

// HashKey hash a string to an int value using fnv32 algorithm
func HashKey(key string) int {
	fnv32 := fnv.New32()
	key = "@#&" + key + "*^%$"
	_, _ = fnv32.Write([]byte(key))
	return int(fnv32.Sum32())
}

func (dict *Dict) countShard(key string) *shard {
	pos := HashKey(key) % dict.size
	return &dict.shards[pos]
}

func (dict *Dict) Get(key string) (any, bool) {

	shard := dict.countShard(key)
	obj, exist := (*shard)[key]
	return obj, exist
}

func (dict *Dict) Set(key string, value any) bool {

	shard := dict.countShard(key)

	(*shard)[key] = value
	dict.count++
	return true
}

func (dict *Dict) SetIfNotExist(key string, value any) bool {

	shard := dict.countShard(key)

	if _, exist := (*shard)[key]; exist {
		return false
	}

	(*shard)[key] = value
	dict.count++
	return true
}

func (dict *Dict) SetIfExist(key string, value any) bool {

	shard := dict.countShard(key)

	if _, exist := (*shard)[key]; exist {
		(*shard)[key] = value

		return true
	}

	return false
}

func (dict *Dict) Update(key string, value any) bool {
	return dict.SetIfExist(key, value)
}

// Delete 成功删除返回 true，无元素返回 false
func (dict *Dict) Delete(key string) bool {
	shard := dict.countShard(key)

	if _, exist := (*shard)[key]; exist {
		delete(*shard, key)
		dict.count--
		return true
	}

	return false
}

func (dict *Dict) Size() int {
	return dict.size
}

func (dict *Dict) Empty() bool {
	return dict.size == 0
}

func (dict *Dict) Clear() {
	*dict = *NewDict(dict.size)
}

func (dict *Dict) Keys() []string {
	keys := make([]string, dict.count)
	i := 0
	for _, shard := range dict.shards {

		for key := range shard {
			keys[i] = key
			i++
		}
	}

	return keys
}

func (dict *Dict) Exist(key string) bool {

	shard := dict.countShard(key)
	_, exist := (*shard)[key]
	return exist
}

func (dict *Dict) Random() (string, any) {

	shard := dict.shards[rand.Int()%dict.size]
	r := rand.Int() % len(shard)

	for k, v := range shard {
		r--
		if r == 0 {
			return k, v
		}
	}
	return "", nil
}
