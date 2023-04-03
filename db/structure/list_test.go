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

func TestListCost(t *testing.T) {

	list := NewList()

	assert.Equal(t, int64(48), list.Cost())

	list.PushBack(Slice("1234567890"))
	assert.Equal(t, int64(48+34), list.Cost())
	list.PopFront()
	assert.Equal(t, int64(48), list.Cost())

	list.PushBack(Slice("1234567890"))
	assert.Equal(t, int64(48+34), list.Cost())
	list.RemoveNode(list.BackNode())
	assert.Equal(t, int64(48), list.Cost())

	list.PushBack(Slice("1234567890"))
	list.PushBack(Slice("1234567890"))
	list.PushBack(Slice("1234567890"))
	list.PushBack(Slice("1234567890"))

	list.Trim(2, -1)
	assert.Equal(t, int64(48+34*2), list.Cost())

}
