package structure

import (
	"testing"
	"time"
)

func DictTest(t *testing.T) {
	dict := NewDict(10)

	str := string("1")
	start := time.Now()
	for i := 1; i < 100; i++ {
		dict.Set(str, 1)
		str += "1"
	}
	end := time.Since(start) / time.Millisecond
	println(end)

	keys := dict.Keys()

	for _, key := range keys {
		println(key)
	}
}
