package structure

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestList(t *testing.T) {
	list := NewList()
	list.PushBack(Int64(0))
	list.PushBack(Int64(1))
	list.PushBack(Int64(2))
	list.PushBack(Int64(3))
	if list.Set(Int64(645), 4) || list.Set(Int64(645), -100) {
		t.Error("Set Failed")
	}

	if !list.Set(Int64(645), -1) && list.Back().(Int64) != 645 {
		t.Error("Set Failed")
	}

	if list.Size() != 4 {
		t.Error("Size Failed")
	}

	if _, n := list.Range(-1, -1); n != 1 {
		t.Error("Range Failed")
	}
	if _, n := list.Range(0, -1); n != 4 {
		t.Error("Range Failed")
	}
	if _, n := list.Range(-100, -1); n != 4 {
		t.Error("Range Failed")
	}
	if v, ok := list.Pos(-1); !ok || v.(Int64) != 645 {
		t.Error("Pos Failed")
	}
	if _, ok := list.Pos(100); ok {
		t.Error("Pos Failed")
	}
}

func TestListB(t *testing.T) {

	list := NewList()

	assert.Nil(t, list.FrontNode())
	assert.Nil(t, list.BackNode())

	list.PushBack(Int64(0))
	list.PushBack(Int64(1))
	list.PushBack(Int64(2))
	list.PushBack(Int64(3))

	assert.Equal(t, Int64(0), list.FrontNode().Value)
	assert.Equal(t, Int64(3), list.BackNode().Value)
	assert.Equal(t, Int64(0), list.Front())
	assert.Equal(t, Int64(3), list.Back())

	n := list.InsertBeforeNode(Int64(-1), list.FrontNode())
	assert.Equal(t, n, list.FrontNode())

	ok := list.InsertBefore(Int64(-1), 0)
	ok = list.InsertAfter(Int64(-1), 0)

	_ = ok

	list.Clear()
	assert.True(t, list.Empty())

	assert.Nil(t, list.PopFront())
	assert.Nil(t, list.PopBack())

	list.PushFront(Int64(0))
	assert.Equal(t, Int64(0), list.PopBack())

}

func TestListCost(t *testing.T) {

	list := NewList()

	assert.Equal(t, int64(48), list.Cost())

	list.PushBack(Slice("1234567890"))
	assert.Equal(t, int64(48+34), list.Cost())
	list.PopFront()
	assert.Equal(t, int64(48), list.Cost())

	list.PushBack(Slice("1234567890"))
	assert.Equal(t, int64(48+34), list.Cost())
	assert.True(t, list.Remove(list.BackNode().Value))
	assert.Equal(t, int64(48), list.Cost())

	list.PushBack(Slice("1234567890"))
	list.PushBack(Slice("1234567890"))
	list.PushBack(Slice("1234567890"))
	list.PushBack(Slice("1234567890"))

	list.Trim(2, -1)
	assert.Equal(t, int64(48+34*2), list.Cost())

}
