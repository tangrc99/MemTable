package cmd

import (
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/db/structure"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/global"
	"testing"
)

func TestCmdKey(t *testing.T) {
	database := db.NewDataBase(1)
	database.SetKey("k1", Slice("v1"))
	database.SetKey("k2", structure.NewList())

	global.UpdateGlobalClock()

	tests := []struct {
		input    [][]byte
		expected resp.RedisData
	}{
		{[][]byte{[]byte("exists"), []byte("k1"), []byte("k3")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("ttl"), []byte("k1")},
			resp.MakeIntData(-1)},

		{[][]byte{[]byte("type"), []byte("k1")},
			resp.MakeStringData("string")},

		{[][]byte{[]byte("type"), []byte("k2")},
			resp.MakeStringData("list")},

		{[][]byte{[]byte("rename"), []byte("k2"), []byte("k3")},
			resp.MakeStringData("OK")},

		{[][]byte{[]byte("rename"), []byte("k2"), []byte("k3")},
			resp.MakeErrorData("error: no such key")},

		{[][]byte{[]byte("exists"), []byte("k1"), []byte("k3")},
			resp.MakeIntData(2)},

		{[][]byte{[]byte("expire"), []byte("k1"), []byte("k3")},
			resp.MakeErrorData("error: k3 is not int")},

		{[][]byte{[]byte("expire"), []byte("k1"), []byte("10")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("ttl"), []byte("k1")},
			resp.MakeIntData(10)},

		{[][]byte{[]byte("expire"), []byte("k45"), []byte("10")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("del"), []byte("k3")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("keys")},
			resp.MakeArrayData([]resp.RedisData{resp.MakeIntData(1), resp.MakeBulkData([]byte("k1"))})},

		{[][]byte{[]byte("randomkey")},
			resp.MakeBulkData([]byte("k1"))},

		{[][]byte{[]byte("pexpire"), []byte("k1"), []byte("10000")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("pexpire"), []byte("1"), []byte("10000")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("pexpire"), []byte("k1"), []byte("ff")},
			resp.MakeErrorData("error: ff is not int")},
	}

	for _, test := range tests {
		cmd, exist := global.FindCommand(string(test.input[0]))
		assert.True(t, exist)
		c := cmd.Function().(command)

		ret := c(database, test.input)
		assert.Equal(t, test.expected, ret)
	}
}
