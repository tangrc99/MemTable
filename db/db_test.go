package db

import (
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/db/eviction"
	"github.com/tangrc99/MemTable/server/global"
	"testing"
	"time"
)

func TestDataBase(t *testing.T) {

	db := NewDataBase(1)
	_, ok := db.GetKey("key")
	assert.False(t, ok)

	ok = db.DeleteKey("key")
	assert.False(t, ok)

	db.SetKey("key", Int64(1))
	assert.True(t, db.RenameKey("key", "key"))

	v, ok := db.GetKey("key")
	assert.True(t, ok)
	assert.Equal(t, Int64(1), v)

	ok = db.DeleteKey("key")
	assert.True(t, ok)

	assert.False(t, db.ExistKey("key"))

	assert.False(t, db.RenameKey("key", "new"))

}

func TestDataBaseTTL(t *testing.T) {

	global.UpdateGlobalClock()

	db := NewDataBase(1)

	assert.True(t, db.SetKeyWithTTL("key", Int64(1), global.Now.Unix()+1))
	assert.True(t, db.SetKey("k1", Int64(1)))
	assert.True(t, db.SetTTL("k1", global.Now.Unix()+2))

	assert.Equal(t, int64(2), db.GetTTL("k1"))

	global.Now = global.Now.Add(time.Second)
	assert.Equal(t, int64(0), db.GetTTL("key"))
	assert.True(t, db.ExistKey("k1"))

	global.Now = global.Now.Add(time.Second)
	assert.Equal(t, int64(-2), db.GetTTL("key"))

	assert.False(t, db.ExistKey("key"))
	global.Now = global.Now.Add(time.Second)

	assert.False(t, db.ExistKey("k1"))

	assert.False(t, db.RemoveTTL("k1"))
	assert.True(t, db.SetKeyWithTTL("key", Int64(1), global.Now.Unix()+1))
	assert.True(t, db.RemoveTTL("key"))

}

func TestDataBaseWatch(t *testing.T) {

	db := NewDataBase(1)

	revised1 := false
	db.Watch("key", &revised1)
	revised2 := false
	db.Watch("key", &revised2)
	db.UnWatch("key", &revised2)
	db.ReviseNotify("key", 0, 0)

	assert.True(t, revised1)
	assert.False(t, revised2)

}

func TestDataBaseRandom(t *testing.T) {

	db := NewDataBase(1)
	db.SetKeyWithTTL("k1", Int64(1), global.Now.Unix()+1)
	db.SetKeyWithTTL("k2", Int64(1), global.Now.Unix()+1)
	db.SetKeyWithTTL("k3", Int64(1), global.Now.Unix()+1)
	db.SetKeyWithTTL("k4", Int64(1), global.Now.Unix()+1)

	keys := []string{"k1", "k2", "k3", "k4"}

	key, ok := db.RandomKey()
	assert.True(t, ok)
	assert.Subset(t, keys, []string{key})

	ks, n := db.Keys(".*")
	assert.Equal(t, 4, n)
	assert.Subset(t, keys, ks)

	global.Now = global.Now.Add(2 * time.Second)

	n = db.CleanExpiredKeys(4)
	assert.Equal(t, 4, n)
	assert.Equal(t, 0, db.Size())
}

func TestDataBaseOptions(t *testing.T) {

	db1 := NewDataBase(1, WithEviction(EvictLRU))
	assert.IsType(t, &eviction.SampleLRU{}, db1.evict)

	db2 := NewDataBase(1, WithEviction(EvictLFU))
	assert.IsType(t, &eviction.TinyLFU{}, db2.evict)

	db3 := NewDataBase(1, WithEviction(NoEviction))
	assert.IsType(t, &eviction.NoEviction{}, db3.evict)

	ch := make(chan string)
	db4 := NewDataBase(1, WithEvictNotification(ch))
	assert.True(t, db4.enableNotification)

	db5 := NewDataBase(1, WithRookies())
	assert.NotNil(t, db5.rookies)
}
