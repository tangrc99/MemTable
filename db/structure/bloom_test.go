package structure

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBloomCreate(t *testing.T) {
}

func TestBloom(t *testing.T) {

	bloom := NewBloomFilter(1000, 0.03)

	data := []uint64{
		654365346, 54365346, 645234432, 12123432, 3213, 12486750, 98567564, 908565687,
		423654567, 345264537, 34265453, 6546, 456, 4537, 653, 72, 346, 547, 643565464536, 456, 547, 65, 7, 546, 234, 645, 65346,
		5465436, 5437, 6547, 980987, 645, 34526, 54, 734, 566, 346, 543, 7, 546, 234, 6, 68, 7, 723, 456, 3564, 872, 6, 123124234, 90987098,
		876098089, 89, 998798089, 98, 98, 709, 80, 786, 906, 789, 6789, 6789, 78, 9678, 9, 65, 876567, 8, 7659, 768, 7, 68, 6758, 765, 8, 768, 5678, 67, 6758,
		1241321, 3245, 123, 412, 3, 1234, 412, 43, 24, 132, 412, 3, 123, 12, 312, 3, 123, 12, 3, 123, 21, 3, 1243, 4, 1, 653476547657, 23534525623}

	for _, d := range data {
		bloom.add(d)
	}
	for _, d := range data {
		assert.True(t, bloom.Has(d))
	}

	for _, d := range data {
		assert.False(t, bloom.AddIfNotHas(d))
	}

	assert.Equal(t, uint64(0x1fff), bloom.Capacity())
	bloom.Clear()
	assert.True(t, bloom.AddIfNotHas(654365346))

	assert.Equal(t, int64(1064), bloom.Cost())
}
