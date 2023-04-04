package structure

import "testing"

func TestSkipList(t *testing.T) {

	skipList := NewSkipList(3)
	skipList.Insert(0.0, Slice("1"))
	skipList.InsertIfNotExist(1.1, Int64(1))
	skipList.Insert(1.2, Slice("222"))

	skipList.Delete(1.1)

	if _, ok := skipList.Get(3.6); ok {
		t.Error("Get Failed")
	}

	if _, size := skipList.Range(99, 100); size != 0 {
		t.Error("Range Failed")
	}

	if skipList.GetPosByKey(35) != -1 {
		t.Error("GetPosByKey Failed")
	}

}
