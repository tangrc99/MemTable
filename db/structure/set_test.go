package structure

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSet(t *testing.T) {

	set := NewSet()

	assert.Equal(t, 0, set.RandomDelete(1))
	assert.Equal(t, map[string]struct{}{}, set.RandomGet(1))
	assert.Equal(t, map[string]struct{}{}, set.RandomPop(1))

	assert.True(t, set.Add("k1"))
	assert.True(t, set.Exist("k1"))
	assert.True(t, set.Delete("k1"))

	keys := []string{"k1", "k2", "k3", "k4"}

	for _, key := range keys {
		set.Add(key)
	}

	{
		ks := set.RandomGet(2)
		var kk []string
		for k := range ks {
			kk = append(kk, k)
		}
		assert.Subset(t, keys, kk)
	}

	{
		ks := set.RandomGet(5)
		var kk []string
		for k := range ks {
			kk = append(kk, k)
		}
		assert.Subset(t, keys, kk)
	}

	assert.Equal(t, 4, set.Size())
	set.RandomDelete(1)
	assert.Equal(t, 3, set.Size())

	{
		ks := set.RandomPop(1)
		var kk []string
		for k := range ks {
			kk = append(kk, k)
		}
		assert.Subset(t, keys, kk)
		assert.Equal(t, 2, set.Size())
	}

	ks, n := set.Keys("")
	assert.Equal(t, 2, n)
	assert.Subset(t, keys, ks)

	ks, n = set.Keys(".*")
	assert.Equal(t, 2, n)
	assert.Subset(t, keys, ks)

	ks, n = set.Keys("p")
	assert.Equal(t, 0, n)

	keysb := [][]byte{[]byte("k1"), []byte("k2"), []byte("k3"), []byte("k4")}
	ksb, n := set.KeysByte(".*")
	assert.Equal(t, 2, n)
	assert.Subset(t, keysb, ksb)
}
