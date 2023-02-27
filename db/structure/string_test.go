package structure

import (
	"testing"
)

func TestString(t *testing.T) {

	slice := []byte("12345")

	println(String(slice).Cost())

	s := String("12345")

	println(s.Cost())

}
