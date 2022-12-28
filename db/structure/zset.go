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

func (zset *ZSet) Insert(key string, score float32) {

	old, exist := zset.dict.Get(key)

	if exist {
		zset.dict.SetIfExist(key, score)
		zset.skipList.Insert(score, key)

	} else {
		// 如果存在则需要先删除跳跃表中原来的键值对
		zset.dict.SetIfNotExist(key, score)
		zset.skipList.Delete(old.(float32))
		zset.skipList.Insert(score, key)
	}
}

func (zset *ZSet) Delete(key string, score float32) {

	ok := zset.dict.Delete(key)
	if ok {
		zset.skipList.Delete(score)
	}
}

func (zset *ZSet) Size() int {
	return zset.skipList.size
}

func (zset *ZSet) GetScoreByKey(key string) (float32, bool) {

	score, ok := zset.dict.Get(key)
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
