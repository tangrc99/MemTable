package structure

// ZSet 使用跳跃表和哈希表实现了 redis 中的 zset 数据结构
type ZSet struct {
	skipList *SkipList // 用于存储 score - key
	dict     *Dict     // 用于存储 key - score
}

// NewZSet 创建一个 ZSet 并返回指针
func NewZSet() *ZSet {
	return &ZSet{
		skipList: NewSkipList(32),
		dict:     NewDict(16),
	}
}

// Add 插入一个键并设置权重，若键已存在，覆盖原有的权重
func (zset *ZSet) Add(score Float32, key string) {

	old, exist := zset.dict.Get(key)

	if exist {

		// 如果存在则需要先删除跳跃表中原来的键值对
		zset.dict.Set(key, score)
		zset.skipList.Delete(old.(Float32))
		zset.skipList.Insert(score, String(key))

	} else {
		zset.dict.Set(key, score)
		zset.skipList.Insert(score, String(key))
	}
}

// AddIfNotExist 插入一个键并设置权重，若键已存在，返回 false
func (zset *ZSet) AddIfNotExist(score Float32, key string) bool {

	_, exist := zset.dict.Get(key)

	if exist {
		return false
	}

	zset.dict.Set(key, score)
	zset.skipList.Insert(score, String(key))

	return true
}

// Delete 删除指定的键，若键不存在，返回 false
func (zset *ZSet) Delete(key string) bool {

	score := zset.dict.DeleteGet(key)

	if score == nil {
		return false
	}

	zset.skipList.Delete(score.(Float32))
	return true
}

// Size 返回键的数量
func (zset *ZSet) Size() int {
	return zset.skipList.size
}

// GetScoreByKey 返回键的权重，若键不存在，返回 -1,false
func (zset *ZSet) GetScoreByKey(key string) (Float32, bool) {

	score, ok := zset.dict.Get(key)
	if !ok {
		return -1, false
	}
	return score.(Float32), ok
}

// GetKeysByRange 返回权重范围内的所有键以及数量
func (zset *ZSet) GetKeysByRange(min, max Float32) ([]string, int) {

	values, size := zset.skipList.Range(min, max)
	keys := make([]string, size)
	for i := 0; i < size; i++ {
		keys[i] = string(values[i].(String))
	}

	return keys, size
}

// CountByRange 返回权重范围内所有键的数量
func (zset *ZSet) CountByRange(min, max Float32) int {
	return zset.skipList.CountByRange(min, max)
}

// PosByScore 获取权重值的排序位置，若权重不存在，返回-1
func (zset *ZSet) PosByScore(score Float32) int {
	return zset.skipList.GetPosByKey(score)
}

// ReviseScore 修改键的权重值，若键不存在，返回 false
func (zset *ZSet) ReviseScore(key string, score Float32) bool {
	old, exist := zset.dict.Get(key)

	if exist {

		return false

	}

	if old.(Float32) == score {
		return true
	}

	zset.skipList.Delete(old.(Float32))
	zset.skipList.Insert(score, String(key))
	return true
}

// IncrScore 将键的权重值增值指定的 increment，若键不存在，返回 false
func (zset *ZSet) IncrScore(key string, increment Float32) (Float32, bool) {
	old, exist := zset.dict.Get(key)

	if !exist {
		return -1, false
	}

	if increment == 0 {
		return old.(Float32), true
	}

	zset.dict.Set(key, old.(Float32)+increment)
	zset.skipList.Delete(old.(Float32))
	zset.skipList.Insert(increment+old.(Float32), String(key))
	return increment + old.(Float32), true
}

// DeleteRange 删除指定位置范围内的所有键，并返回删除数量
func (zset *ZSet) DeleteRange(start, end int) int {
	keys, deleted := zset.skipList.DeletePos(start, end)

	for _, key := range keys {
		zset.dict.Delete(string(key.(String)))
	}

	return deleted
}

// DeleteRangeByScore 删除权重范围内的所有键，返回删除数量
func (zset *ZSet) DeleteRangeByScore(min, max Float32) int {
	keys, deleted := zset.skipList.DeleteRange(min, max)

	for _, key := range keys {
		zset.dict.Delete(string(key.(String)))
	}

	return deleted
}

// Pos 返回指定位置范围内的所有键
func (zset *ZSet) Pos(start, end int) ([]Object, int) {
	return zset.skipList.Pos(start, end)
}

func (zset *ZSet) Cost() int64 {
	return zset.skipList.Cost() + zset.dict.Cost()
}
