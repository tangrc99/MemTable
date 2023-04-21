package structure

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestZSet(t *testing.T) {

	zset := NewZSet()

	zset.Add(1.1, "k1")
	assert.Equal(t, 0, zset.PosByScore(1.1))
	assert.False(t, zset.AddIfNotExist(1.2, "k1"))
	assert.True(t, zset.AddIfNotExist(1.2, "k2"))

	objs, n := zset.Pos(0, -1)
	assert.Equal(t, 2, n)
	assert.Equal(t, []Object{String("k1"), String("k2")}, objs)

	assert.False(t, zset.Delete("k3"))

	keys, n := zset.GetKeysByRange(0, 1.1)
	assert.Equal(t, 1, n)
	assert.Equal(t, []string{"k1"}, keys)

	assert.Equal(t, 2, zset.CountByRange(0, 100))

	assert.True(t, zset.ReviseScore("k1", 1.3))
	assert.False(t, zset.ReviseScore("k43", 1.3))

	zset.DeleteRange(0, -1)

	assert.Equal(t, 0, zset.Size())

	zset.Add(1.1, "k1")
	assert.Zero(t, zset.DeleteRangeByScore(1.2, 100))

	assert.Equal(t, 1, zset.DeleteRangeByScore(1, 1.1))

	zset.Add(1.1, "k1")
	zset.Add(1.3, "k1")
	score, ok := zset.GetScoreByKey("k5")
	assert.False(t, ok)
	score, ok = zset.GetScoreByKey("k1")
	assert.True(t, ok)
	assert.Equal(t, Float32(1.3), score)

	score, ok = zset.IncrScore("k5", 1)
	assert.False(t, ok)

	score, ok = zset.IncrScore("k1", 1)
	assert.True(t, ok)
	assert.Equal(t, Float32(2.3), score)

	assert.True(t, zset.Delete("k1"))
}
