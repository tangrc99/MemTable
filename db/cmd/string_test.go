package cmd

import (
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/global"
	"testing"
)

func TestCmdString(t *testing.T) {
	database := db.NewDataBase(1)

	global.UpdateGlobalClock()

	tests := []struct {
		input    [][]byte
		expected resp.RedisData
	}{
		{[][]byte{[]byte("set"), []byte("k1"), []byte("v1")},
			resp.MakeStringData("OK")},

		{[][]byte{[]byte("get"), []byte("k1")},
			resp.MakeBulkData([]byte("v1"))},

		{[][]byte{[]byte("get"), []byte("k2")},
			resp.MakeStringData("nil")},

		{[][]byte{[]byte("getset"), []byte("k1"), []byte("v11")},
			resp.MakeStringData("OK")},

		{[][]byte{[]byte("strlen"), []byte("k1")},
			resp.MakeIntData(3)},

		{[][]byte{[]byte("strlen"), []byte("k11")},
			resp.MakeIntData(-1)},

		{[][]byte{[]byte("getrange"), []byte("k1"), []byte("0"), []byte("-1")},
			resp.MakeBulkData([]byte{})},

		{[][]byte{[]byte("getrange"), []byte("k1"), []byte("0"), []byte("1")},
			resp.MakeBulkData([]byte("v"))},

		{[][]byte{[]byte("getrange"), []byte("k1"), []byte("0"), []byte("100")},
			resp.MakeBulkData([]byte("v11"))},

		{[][]byte{[]byte("getrange"), []byte("k1"), []byte("ff"), []byte("100")},
			resp.MakeErrorData("ERR value is not an integer or out of range")},

		{[][]byte{[]byte("getrange"), []byte("k1"), []byte("0"), []byte("ff")},
			resp.MakeErrorData("ERR value is not an integer or out of range")},

		{[][]byte{[]byte("setrange"), []byte("k1"), []byte("0"), []byte("100")},
			resp.MakeIntData(3)},

		{[][]byte{[]byte("setrange"), []byte("k1"), []byte("ff"), []byte("100")},
			resp.MakeErrorData("ERR value is not an integer or out of range")},

		{[][]byte{[]byte("mset"), []byte("k1"), []byte("v1"), []byte("k2"), []byte("v2")},
			resp.MakeStringData("OK")},

		{[][]byte{[]byte("mget"), []byte("k1"), []byte("k2")},
			resp.MakeArrayData([]resp.RedisData{resp.MakeBulkData([]byte("v1")), resp.MakeBulkData([]byte("v2"))})},

		{[][]byte{[]byte("append"), []byte("k1"), []byte("k2")},
			resp.MakeIntData(4)},

		{[][]byte{[]byte("set"), []byte("k1"), []byte("0")},
			resp.MakeStringData("OK")},

		{[][]byte{[]byte("incr"), []byte("k1")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("incrby"), []byte("k1"), []byte("2")},
			resp.MakeIntData(3)},

		{[][]byte{[]byte("incrby"), []byte("k1"), []byte("2dfg")},
			resp.MakeErrorData("ERR value is not an integer or out of range")},

		{[][]byte{[]byte("decr"), []byte("k1")},
			resp.MakeIntData(2)},

		{[][]byte{[]byte("decrby"), []byte("k1"), []byte("2")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("decrby"), []byte("k1"), []byte("2dfg")},
			resp.MakeErrorData("ERR value is not an integer or out of range")},
	}

	for _, test := range tests {
		cmd, exist := global.FindCommand(string(test.input[0]))
		assert.True(t, exist)
		c := cmd.Function().(command)

		ret := c(database, test.input)
		assert.Equal(t, test.expected, ret)
	}
}
