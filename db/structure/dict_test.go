package structure

import (
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
