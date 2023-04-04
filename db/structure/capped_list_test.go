package structure

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCappedList(t *testing.T) {

	cl := NewCappedList(3)

	cl.Append(Slice("1"))
	cl.Append(Slice("2"))

	assert.Equal(t, 2, cl.Size())

	cl.Append(Slice("3"))

	objs := cl.GetN(3)

	assert.Equal(t, []Object{Slice("1"), Slice("2"), Slice("3")}, objs)

	cl.Append(Slice("4"))

	assert.Equal(t, 3, cl.Size())

	objs = cl.GetN(3)

	assert.Equal(t, []Object{Slice("2"), Slice("3"), Slice("4")}, objs)

	objs = cl.GetN(1)

	assert.Equal(t, []Object{Slice("2")}, objs)

	objs = cl.GetN(100)

	assert.Equal(t, []Object{Slice("2"), Slice("3"), Slice("4")}, objs)
}

func TestCappedListCost(t *testing.T) {

	cl := NewCappedList(3)

	assert.Equal(t, cappedListBasicCost, cl.Cost())

	cl.Append(Slice("1"))

	assert.Equal(t, cappedListBasicCost+cappedListNodeBasicCost+1, cl.Cost())

	cl.Append(Slice("2"))
	cl.Append(Slice("3"))

	oldCost := cl.Cost()

	cl.Append(Slice("4"))

	newCost := cl.Cost()

	assert.Equal(t, oldCost, newCost)
}
