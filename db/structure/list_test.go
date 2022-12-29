package structure

func ListTest() {
	list := NewList()
	list.PushBack(0)
	list.PushBack(1)
	list.PushBack(2)
	list.PushBack(3)
	list.Set(645, 4)
	//nums := list.RemoveValue(66, 5)
	//println(nums)

	//list.InsertBefore(4, -1)
	//
	//list.Set(-1, 0)
	//
	//list.InsertBefore(-2, -1)
	//
	//list.RemoveValue(3, 10)

	values, ok := list.Range(3, 100)
	if ok {
		for _, v := range values {
			println(v.(int))
		}
	}
}
