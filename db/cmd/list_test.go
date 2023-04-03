package cmd

import (
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/db/structure"
	"testing"
)

func initDatabase() *db.DataBase {

	database := db.NewDataBase(1)
	database.SetKey("test", structure.NewList())

	return database
}

func TestCmdListLPush(t *testing.T) {

	database := db.NewDataBase(1)
	list := structure.NewList()
	database.SetKey("test", list)

	lPush(database, [][]byte{
		[]byte("lpush"), []byte("test"), []byte("1"), []byte("2"), []byte("3"), []byte("4"),
	})

	vals, num := list.Range(0, -1)

	assert.Equal(t, 4, num)
	assert.Equal(t, structure.Slice("4"), vals[0])
	assert.Equal(t, structure.Slice("1"), vals[3])
}

func TestCmdListRPush(t *testing.T) {

	database := db.NewDataBase(1)
	list := structure.NewList()
	database.SetKey("test", list)

	rPush(database, [][]byte{
		[]byte("rpush"), []byte("test"), []byte("1"), []byte("2"), []byte("3"), []byte("4"),
	})

	vals, num := list.Range(0, -1)

	assert.Equal(t, 4, num)
	assert.Equal(t, structure.Slice("1"), vals[0])
	assert.Equal(t, structure.Slice("4"), vals[3])
}

func TestCmdListLLen(t *testing.T) {
	database := db.NewDataBase(1)
	list := structure.NewList()
	database.SetKey("test", list)

	assert.Equal(t, 0, list.Size())

	rPush(database, [][]byte{
		[]byte("rpush"), []byte("test"), []byte("1"), []byte("2"), []byte("3"), []byte("4"),
	})

	assert.Equal(t, 4, list.Size())

}
