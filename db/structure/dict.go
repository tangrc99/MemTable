package structure

import (
	"hash/fnv"
)

const MaxConSize = uint(1<<31 - 1)

type shard = map[string]any

type Dict struct {
	shards []shard // 存储键值对
	size   uint    // table 分区数量
	count  uint64  // 键值对数量
}

func NewDict(size uint) *Dict {
	if size <= 0 || size > MaxConSize {
		size = MaxConSize
	}
	dict := Dict{
		shards: make([]shard, size),
		size:   size,
		count:  0,
	}
	for i := uint(0); i < size; i++ {
		dict.shards[i] = make(map[string]any)
	}
	return &dict
}

// HashKey hash a string to an int value using fnv32 algorithm
func HashKey(key string) uint {
	fnv32 := fnv.New32()
	key = "@#&" + key + "*^%$"
	_, _ = fnv32.Write([]byte(key))
	return uint(fnv32.Sum32())
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

func (dict *Dict) Delete(key string) bool {
	shard := dict.countShard(key)

	if _, exist := (*shard)[key]; exist {
		delete(*shard, key)
		dict.count--
		return true
	}

	return false
}

func (dict *Dict) Size() uint {
	return dict.size
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