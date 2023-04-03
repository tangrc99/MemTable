package structure

// Set 是一个键集合，底层数据结构为哈希表
type Set struct {
	dict *Dict
}

// NewSet 创建一个 Set 并返回指针
func NewSet() *Set {
	return &Set{
		dict: NewDict(16),
	}
}

// Add 将指定键插入到集合中，若键已存在将返回 false
func (set *Set) Add(key string) bool {
	return set.dict.SetIfNotExist(key, Nil{})
}

// Delete 将指定键从集合中删除，若键不存在将返回 false
func (set *Set) Delete(key string) bool {
	return set.dict.Delete(key)
}

// Exist 判断键是否存在于集合中
func (set *Set) Exist(key string) bool {
	return set.dict.Exist(key)
}

// Size 返回集合键数量
func (set *Set) Size() int {
	return set.dict.count
}

// RandomDelete 随机删除集合中指定数量的键，返回删除的数量
func (set *Set) RandomDelete(nums int) int {
	if set.dict.Empty() {
		return 0
	}

	keys := set.dict.RandomKeys(nums)

	deleted := 0

	for key, _ := range keys {
		if set.dict.Delete(key) {
			deleted++
		}
	}

	return deleted
}

// RandomGet 随机获取集合中指定数量的键
func (set *Set) RandomGet(nums int) map[string]struct{} {
	if set.dict.Empty() {
		return make(map[string]struct{})
	}

	keys := set.dict.RandomKeys(nums)
	return keys
}

// RandomPop 随机删除集合中指定数量的键，返回被删除的键
func (set *Set) RandomPop(nums int) map[string]struct{} {
	if set.dict.Empty() {
		return make(map[string]struct{})
	}

	keys := set.dict.RandomKeys(nums)

	for key := range keys {
		set.dict.Delete(key)
	}

	return keys
}

// Keys 返回通过正则表达式匹配的所有键
func (set *Set) Keys(pattern string) ([]string, int) {
	return set.dict.Keys(pattern)
}

// KeysByte 返回通过正则表达式匹配的所有键，键以[]byte形式返回
func (set *Set) KeysByte(pattern string) ([][]byte, int) {
	return set.dict.KeysByte(pattern)
}

func (set *Set) Cost() int64 {

	// TODO:
	return -1
}
