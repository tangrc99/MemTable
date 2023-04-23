package cmd

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/global"
	"testing"
)

func TestCmdList(t *testing.T) {
	database := db.NewDataBase(1)

	tests := []struct {
		input    [][]byte
		expected resp.RedisData
	}{

		{[][]byte{[]byte("llen"), []byte("test")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("lpop"), []byte("test")},
			resp.MakeStringData("nil")},

		{[][]byte{[]byte("rpop"), []byte("test")},
			resp.MakeStringData("nil")},

		{[][]byte{[]byte("lindex"), []byte("test"), []byte("1")},
			resp.MakeStringData("nil")},

		{[][]byte{[]byte("lpos"), []byte("test"), []byte("1")},
			resp.MakeStringData("nil")},

		{[][]byte{[]byte("lset"), []byte("test"), []byte("1"), []byte("1")},
			resp.MakeErrorData("ERR no such key")},

		{[][]byte{[]byte("lpush"), []byte("test"), []byte("2"), []byte("1")},
			resp.MakeIntData(2)},

		{[][]byte{[]byte("rpush"), []byte("test"), []byte("3"), []byte("4")},
			resp.MakeIntData(2)},

		{[][]byte{[]byte("llen"), []byte("test")},
			resp.MakeIntData(4)},

		{[][]byte{[]byte("lpos"), []byte("test"), []byte("10")},
			resp.MakeStringData("nil")},

		{[][]byte{[]byte("lpos"), []byte("test"), []byte("3")},
			resp.MakeIntData(2)},

		{[][]byte{[]byte("lset"), []byte("test"), []byte("f"), []byte("1")},
			resp.MakeErrorData("ERR value is not an integer or out of range")},

		{[][]byte{[]byte("lset"), []byte("test"), []byte("10"), []byte("1")},
			resp.MakeErrorData("ERR index out of range")},

		{[][]byte{[]byte("lset"), []byte("test"), []byte("3"), []byte("4")},
			resp.MakeStringData("OK")},

		{[][]byte{[]byte("lrange"), []byte("test"), []byte("f"), []byte("4")},
			resp.MakeErrorData("ERR value is not an integer or out of range")},

		{[][]byte{[]byte("lrange"), []byte("test"), []byte("3"), []byte("f")},
			resp.MakeErrorData("ERR value is not an integer or out of range")},

		{[][]byte{[]byte("lrange"), []byte("test"), []byte("0"), []byte("0")},
			resp.MakeArrayData([]resp.RedisData{resp.MakeBulkData([]byte("1"))})},

		{[][]byte{[]byte("lrem"), []byte("test"), []byte("100"), []byte("3")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("lrem"), []byte("test"), []byte("f"), []byte("3")},
			resp.MakeErrorData("ERR value is not an integer or out of range")},

		{[][]byte{[]byte("lpop"), []byte("test"), []byte("1")},
			resp.MakeArrayData([]resp.RedisData{
				resp.MakeBulkData([]byte("1")),
			})},

		{[][]byte{[]byte("lpop"), []byte("test"), []byte("f")},
			resp.MakeErrorData("ERR value is not an integer or out of range")},

		{[][]byte{[]byte("rpop"), []byte("test"), []byte("1")},
			resp.MakeArrayData([]resp.RedisData{
				resp.MakeBulkData([]byte("4")),
			})},

		{[][]byte{[]byte("rpop"), []byte("test"), []byte("f")},
			resp.MakeErrorData("ERR value is not an integer or out of range")},

		{[][]byte{[]byte("ltrim"), []byte("test"), []byte("f"), []byte("1")},
			resp.MakeErrorData("ERR value is not an integer or out of range")},

		{[][]byte{[]byte("ltrim"), []byte("test"), []byte("1"), []byte("f")},
			resp.MakeErrorData("ERR value is not an integer or out of range")},

		{[][]byte{[]byte("ltrim"), []byte("test"), []byte("-1"), []byte("0")},
			resp.MakeStringData("OK")},

		{[][]byte{[]byte("llen"), []byte("test")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("rpush"), []byte("test"), []byte("1"), []byte("2"), []byte("3"), []byte("4")},
			resp.MakeIntData(4)},

		{[][]byte{[]byte("lmove"), []byte("test"), []byte("l"), []byte("LEFT"), []byte("RIGHT")},
			resp.MakeStringData("OK")},

		{[][]byte{[]byte("lmove"), []byte("test"), []byte("l"), []byte("LEFT"), []byte("LEFT")},
			resp.MakeStringData("OK")},

		{[][]byte{[]byte("lmove"), []byte("test"), []byte("l"), []byte("RIGHT"), []byte("LEFT")},
			resp.MakeStringData("OK")},

		{[][]byte{[]byte("lmove"), []byte("test"), []byte("l"), []byte("RIGHT"), []byte("RIGHT")},
			resp.MakeStringData("OK")},

		{[][]byte{[]byte("llen"), []byte("test")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("lindex"), []byte("l"), []byte("1")},
			resp.MakeBulkData([]byte("2"))},

		{[][]byte{[]byte("lindex"), []byte("l"), []byte("100")},
			resp.MakeStringData("nil")},

		{[][]byte{[]byte("lindex"), []byte("l"), []byte("f")},
			resp.MakeErrorData("ERR value is not an integer or out of range")},

		{[][]byte{[]byte("lpop"), []byte("l"), []byte("4")},
			resp.MakeArrayData([]resp.RedisData{
				resp.MakeBulkData([]byte("4")),
				resp.MakeBulkData([]byte("2")),
				resp.MakeBulkData([]byte("1")),
				resp.MakeBulkData([]byte("3")),
			})},
	}

	for i, test := range tests {
		cmd, exist := global.FindCommand(string(test.input[0]))
		assert.True(t, exist)
		c := cmd.Function().(command)

		ret := c(database, test.input)
		if !assert.Equal(t, test.expected, ret) {
			fmt.Printf("test case %d", i)
		}
	}
}
