package cmd

import (
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/db/structure"
	"testing"
)

func TestCmdHashHSet(t *testing.T) {

	database := db.NewDataBase(1)
	dict := structure.NewDict(1)
	database.SetKey("test", dict)

	hSet(database, [][]byte{
		[]byte("hset"), []byte("test"), []byte("key"), []byte("value"),
	})

	obj, ok := dict.Get("key")

	assert.Equal(t, true, ok)
	assert.Equal(t, structure.Slice("value"), obj)
}
