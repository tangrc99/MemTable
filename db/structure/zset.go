package structure

type ZSet struct {
	skipList *SkipList // 用于存储 score - key
	dict     *Dict     // 用于存储 key - score
}

func NewZSet() *ZSet {
	return &ZSet{
		skipList: NewSkipList(32),
		dict:     NewDict(16),
	}
}

func (zset *ZSet) Add(score float32, key string) {

	old, exist := zset.dict.Get(key)

	if exist {

		// 如果存在则需要先删除跳跃表中原来的键值对
		zset.dict.Set(key, score)
		zset.skipList.Delete(old.(float32))
		zset.skipList.Insert(score, key)

	} else {
		zset.dict.Set(key, score)
		zset.skipList.Insert(score, key)
	}
}

func (zset *ZSet) AddIfNotExist(score float32, key string) bool {

	_, exist := zset.dict.Get(key)

	if exist {

		return false

	}

	zset.dict.Set(key, score)
	zset.skipList.Insert(score, key)

	return true
}

func (zset *ZSet) Delete(key string) bool {

	score := zset.dict.DeleteGet(key)

	if score == nil {
		return false
	}

	zset.skipList.Delete(score.(float32))
	return true
}

func (zset *ZSet) Size() int {
	return zset.skipList.size
}

func (zset *ZSet) GetScoreByKey(key string) (float32, bool) {

	score, ok := zset.dict.Get(key)
	if !ok {
		return -1, false
	}
	return score.(float32), ok
}

func (zset *ZSet) GetKeysByRange(min, max float32) ([]string, int) {

	values, size := zset.skipList.Range(min, max)
	keys := make([]string, size)
	for i := 0; i < size; i++ {
		keys[i] = values[i].(string)
	}

	return keys, size
}

// CountByRange returns the num of keys between range [min,max]
func (zset *ZSet) CountByRange(min, max float32) int {
	return zset.skipList.CountByRange(min, max)
}

func (zset *ZSet) PosByScore(score float32) int {
	return zset.skipList.GetPosByKey(score)
}

func (zset *ZSet) ReviseScore(key string, score float32) bool {
	old, exist := zset.dict.Get(key)

	if exist {

		return false

	}

	if old.(float32) == score {
		return true
	}

	zset.skipList.Delete(old.(float32))
	zset.skipList.Insert(score, key)
	return true
}

func (zset *ZSet) IncrScore(key string, increment float32) (float32, bool) {
	old, exist := zset.dict.Get(key)

	if !exist {
		return -1, false
	}

	if increment == 0 {
		return old.(float32), true
	}

	zset.dict.Set(key, old.(float32)+increment)
	zset.skipList.Delete(old.(float32))
	zset.skipList.Insert(increment+old.(float32), key)
	return increment + old.(float32), true
}

func (zset *ZSet) DeleteRange(start, end int) int {
	keys, deleted := zset.skipList.DeletePos(start, end)

	for _, key := range keys {
		zset.dict.Delete(key.(string))
	}

	return deleted
}

func (zset *ZSet) DeleteRangeByScore(min, max float32) int {
	keys, deleted := zset.skipList.DeleteRange(min, max)

	for _, key := range keys {
		zset.dict.Delete(key.(string))
	}

	return deleted
}

func (zset *ZSet) Pos(start, end int) ([]any, int) {
	return zset.skipList.Pos(start, end)
}
