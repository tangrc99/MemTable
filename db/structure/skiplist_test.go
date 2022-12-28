package structure

func SkipListTest() {
	skipList := NewSkipList(3)
	skipList.Insert(0.0, "1")
	skipList.InsertIfNotExist(1.1, 1)
	skipList.Insert(1.2, "222")

	skipList.Delete(1.1)

	v, ok := skipList.Get(3.6)
	if ok {
		println(v)
	} else {
		println("not found")
	}

	values, size := skipList.Range(99, 100)
	for i := 0; i < size; i++ {
		println(values[i])
	}

	println(skipList.GetPosByKey(35))

}
