package structure

type Set struct {
	dict *Dict
}

func NewSet() *Set {
	return &Set{
		dict: NewDict(16),
	}
}

func (set *Set) Add(key string) {
	set.dict.Set(key, struct{}{})
}

func (set *Set) Delete(key string) bool {
	return set.dict.Delete(key)
}

func (set *Set) Exist(key string) bool {
	return set.dict.Exist(key)
}

func (set *Set) Size() int {
	return set.dict.size
}

func (set *Set) RandomDelete() string {
	if set.dict.Empty() {
		return ""
	}

	key, _ := set.dict.Random()
	set.dict.Delete(key)
	return key
}

func (set *Set) RandomGet() string {
	if set.dict.Empty() {
		return ""
	}

	key, _ := set.dict.Random()
	return key
}

func (set *Set) Keys(pattern string) ([]string, int) {
	return set.dict.Keys(pattern)
}
