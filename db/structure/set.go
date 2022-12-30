package structure

type Set struct {
	dict *Dict
}

func NewSet() *Set {
	return &Set{
		dict: NewDict(16),
	}
}

func (set *Set) Add(key string) bool {
	return set.dict.SetIfNotExist(key, struct{}{})
}

func (set *Set) Delete(key string) bool {
	return set.dict.Delete(key)
}

func (set *Set) Exist(key string) bool {
	return set.dict.Exist(key)
}

func (set *Set) Size() int {
	return set.dict.count
}

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

func (set *Set) RandomGet(nums int) map[string]struct{} {
	if set.dict.Empty() {
		return make(map[string]struct{})
	}

	keys := set.dict.RandomKeys(nums)
	return keys
}

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

func (set *Set) Keys(pattern string) ([]string, int) {
	return set.dict.Keys(pattern)
}

func (set *Set) KeysByte(pattern string) ([][]byte, int) {
	return set.dict.KeysByte(pattern)
}
