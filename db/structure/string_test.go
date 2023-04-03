package structure

import (
	"testing"
)

func TestString(t *testing.T) {

	slice := []byte("12345")

	println(Slice(slice).Cost())

	s := Slice("12345")

	println(s.Cost())

}
