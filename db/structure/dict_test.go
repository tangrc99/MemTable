package structure

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDict(t *testing.T) {
	dict := NewDict(10)

	dict.Set("1", Slice("1"))

	if v, ok := dict.Get("1"); !ok || string(v.(Slice)) != "1" {
		t.Error("Set Get failed")
	}

	if dict.Size() != 1 {
		t.Error("Size failed")
	}

	if len(dict.Random(100)) != 1 {
		t.Error("Random failed")
	}

	if dict.SetIfNotExist("1", Slice("1")) {
		t.Error("SetIfNotExist failed")
	}

	if !dict.SetIfExist("1", Slice("2")) {
		t.Error("SetIfExist failed")
	}

	if v, ok := dict.Get("1"); !ok || string(v.(Slice)) != "2" {
		t.Error("SetIfNotExist failed")
	}

	if !dict.Set("1", Slice("3")) {
		t.Error("Set failed")
	}

	if v, ok := dict.Get("1"); !ok || string(v.(Slice)) != "3" {
		t.Error("Set failed")
	}

	if !dict.Delete("1") || dict.Size() != 0 || !dict.Empty() {
		t.Error("Delete failed")
	}

	if len(dict.Random(100)) != 0 {
		t.Error("Random failed")
	}

}

func TestDictCost(t *testing.T) {
	dict := NewDict(1)

	assert.Equal(t, int64(56), dict.Cost())

	dict.Set("12345", Slice("12345"))
	assert.Equal(t, int64(66), dict.Cost())

	dict.SetIfExist("12345", Slice("1234567890"))
	assert.Equal(t, int64(71), dict.Cost())

	dict.SetIfNotExist("12345", Slice("1234567890"))
	assert.Equal(t, int64(71), dict.Cost())

	dict.Delete("12345")
	assert.Equal(t, int64(56), dict.Cost())
}

func TestDictCostWithType(t *testing.T) {
	dict := NewDict(1)

	assert.Equal(t, int64(56), dict.Cost())

	dict.Set("12345", Slice("12345"))
	assert.Equal(t, int64(66), dict.Cost())
	list := NewList()
	dict.Set("list", list)
	assert.Equal(t, int64(66+4+list.Cost()), dict.Cost())

	hash := NewDict(1)
	dict.Set("hash", hash)
	assert.Equal(t, int64(74+56+list.Cost()), dict.Cost())

	dict.Clear()
	assert.Equal(t, int64(56), dict.Cost())

}
