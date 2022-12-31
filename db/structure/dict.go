package structure

import (
	"MemTable/logger"
	"hash/fnv"
	"math/rand"
	"regexp"
	"time"
)

const MaxConSize = int(1<<31 - 1)

type Shard = map[string]any

type Dict struct {
	shards []Shard // 存储键值对
	size   int     // table 分区数量
	count  int     // 键值对数量
}

func NewDict(size int) *Dict {
	if size <= 0 || size > MaxConSize {
		size = MaxConSize
	}
	dict := Dict{
		shards: make([]Shard, size),
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

func (dict *Dict) countShard(key string) *Shard {
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

func (dict *Dict) DeleteGet(key string) any {
	shard := dict.countShard(key)

	if value, exist := (*shard)[key]; exist {
		delete(*shard, key)
		dict.count--
		return value
	}

	return nil
}

func (dict *Dict) Size() int {
	return dict.count
}

func (dict *Dict) Empty() bool {
	return dict.count == 0
}

func (dict *Dict) Clear() {
	*dict = *NewDict(dict.size)
}

// Keys 返回全部键
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

// KeysWithTTL 返回全部未过期键，ttl 记录过期时间
func (dict *Dict) KeysWithTTL(ttl *Dict, pattern string) ([]string, int) {

	now := time.Now().Unix()

	keys := make([]string, dict.count)
	i := 0
	for _, shard := range dict.shards {

		for key := range shard {

			tp, exist := ttl.Get(key)
			if exist && tp.(int64) < now {
				// 如果过期需要删除
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

func (dict *Dict) KeysWithTTLByte(ttl *Dict, pattern string) ([][]byte, int) {

	now := time.Now().Unix()

	keys := make([][]byte, dict.count)
	i := 0
	for _, shard := range dict.shards {

		for key := range shard {

			tp, exist := ttl.Get(key)
			if exist && tp.(int64) < now {
				// 如果过期需要删除
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

func (dict *Dict) Exist(key string) bool {

	shard := dict.countShard(key)
	_, exist := (*shard)[key]
	return exist
}

//func (dict *Dict) Random() (string, any) {
//
//	shard := dict.shards[rand.Int()%dict.size]
//	r := rand.Int() % len(shard)
//
//	for k, v := range shard {
//		r--
//		if r == 0 {
//			return k, v
//		}
//	}
//	return "", nil
//}

func (dict *Dict) Random(num int) map[string]any {

	selected := make(map[string]any)

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
			for k, _ := range dict.shards[i] {
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

func (dict *Dict) GetAll() (*[]map[string]any, int) {
	return &dict.shards, dict.count
}
