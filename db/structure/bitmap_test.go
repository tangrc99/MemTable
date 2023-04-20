package structure

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBitMap(t *testing.T) {

	bitmap := NewBitMap(100)

	bitmap.Set(1, 1)

	assert.Equal(t, byte(1), bitmap.Get(1))
	assert.Equal(t, byte(0), bitmap.Get(1234))

	assert.Equal(t, 13, bitmap.ByteLen())

	assert.Equal(t, byte(1), bitmap.GetSet(1, 1))

	bitmap.Set(6, 1)

	assert.Equal(t, 2, bitmap.Count(0, 0))

	assert.Equal(t, 0, bitmap.Pos(0, 0, 0))
	assert.Equal(t, 1, bitmap.Pos(1, 0, 0))

	bitmap.RangeSet(1, 0, 99)

	assert.Equal(t, byte(1), bitmap.Get(43))

	b := []byte{1, 1, 1, 1, 1, 1, 1}
	bitmap = NewBitMapFromBytes(b)
	assert.Equal(t, byte(0), bitmap.Get(2))

}
