package cmd

import (
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/db/structure"
	"github.com/tangrc99/MemTable/resp"
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

func TestCmdListLPop(t *testing.T) {
	database := db.NewDataBase(1)
	list := structure.NewList()
	database.SetKey("test", list)

	err := lPop(database, [][]byte{
		[]byte("lpop"), []byte("test"), []byte("f"),
	})

	assert.IsType(t, &resp.ErrorData{}, err)

	empty := lPop(database, [][]byte{
		[]byte("lpop"), []byte("test"), []byte("1"),
	})

	assert.Equal(t, []byte(""), empty.ByteData())

	rPush(database, [][]byte{
		[]byte("rpush"), []byte("test"), []byte("1"), []byte("2"), []byte("3"), []byte("4"),
	})

	resp := lPop(database, [][]byte{
		[]byte("lpop"), []byte("test"), []byte("4"),
	})

	assert.Equal(t, []byte("1234"), resp.ByteData())
}

func TestCmdListRPop(t *testing.T) {
	database := db.NewDataBase(1)
	list := structure.NewList()
	database.SetKey("test", list)

	err := rPop(database, [][]byte{
		[]byte("rpop"), []byte("test"), []byte("f"),
	})

	assert.IsType(t, &resp.ErrorData{}, err)

	empty := rPop(database, [][]byte{
		[]byte("rpop"), []byte("test"), []byte("1"),
	})

	assert.Equal(t, []byte(""), empty.ByteData())

	lPush(database, [][]byte{
		[]byte("lpush"), []byte("test"), []byte("1"), []byte("2"), []byte("3"), []byte("4"),
	})

	resp := rPop(database, [][]byte{
		[]byte("rpop"), []byte("test"), []byte("4"),
	})

	assert.Equal(t, []byte("1234"), resp.ByteData())
}

func TestCmdListLIndex(t *testing.T) {
	database := db.NewDataBase(1)
	list := structure.NewList()
	database.SetKey("test", list)

	err := lIndex(database, [][]byte{
		[]byte("lindex"), []byte("test"), []byte("f"),
	})

	assert.IsType(t, &resp.ErrorData{}, err)

	empty := lIndex(database, [][]byte{
		[]byte("lindex"), []byte("test"), []byte("1"),
	})

	assert.Equal(t, []byte("nil"), empty.ByteData())

	rPush(database, [][]byte{
		[]byte("rpush"), []byte("test"), []byte("1"), []byte("2"), []byte("3"), []byte("4"),
	})

	index0 := lIndex(database, [][]byte{
		[]byte("lindex"), []byte("test"), []byte("0"),
	})
	assert.Equal(t, []byte("1"), index0.ByteData())

	index2 := lIndex(database, [][]byte{
		[]byte("lindex"), []byte("test"), []byte("2"),
	})
	assert.Equal(t, []byte("3"), index2.ByteData())

	index_1 := lIndex(database, [][]byte{
		[]byte("lindex"), []byte("test"), []byte("-1"),
	})
	assert.Equal(t, []byte("4"), index_1.ByteData())

}

//
//func TestCmdListLPos(t *testing.T) {
//	database := db.NewDataBase(1)
//	list := structure.NewList()
//	database.SetKey("test", list)
//
//}
//
//func TestCmdListLSet(t *testing.T) {
//	database := db.NewDataBase(1)
//	list := structure.NewList()
//	database.SetKey("test", list)
//
//}
//
//func TestCmdListLRem(t *testing.T) {
//	database := db.NewDataBase(1)
//	list := structure.NewList()
//	database.SetKey("test", list)
//
//}
//
//func TestCmdListLRange(t *testing.T) {
//	database := db.NewDataBase(1)
//	list := structure.NewList()
//	database.SetKey("test", list)
//
//}
//
//func TestCmdListLTrim(t *testing.T) {
//	database := db.NewDataBase(1)
//	list := structure.NewList()
//	database.SetKey("test", list)
//
//}
//
//func TestCmdListLMove(t *testing.T) {
//	database := db.NewDataBase(1)
//	list := structure.NewList()
//	database.SetKey("test", list)
//
//}

func TestCmdListNotify(t *testing.T) {

	database := db.NewDataBase(1)
	list := structure.NewList()
	database.SetKey("test", list)

	revised := false
	database.Watch("test", &revised)

	oldCost := database.Cost()

	lPush(database, [][]byte{
		[]byte("lpush"), []byte("test"), []byte("1"), []byte("2"), []byte("3"), []byte("4"),
	})

	assert.Equal(t, true, revised)
	assert.Positive(t, database.Cost()-oldCost)
}
