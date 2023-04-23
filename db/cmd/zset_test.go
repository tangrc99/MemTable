package cmd

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/global"
	"testing"
)

func TestCmdZSet(t *testing.T) {
	database := db.NewDataBase(1)

	global.UpdateGlobalClock()

	tests := []struct {
		input    [][]byte
		expected resp.RedisData
	}{
		{[][]byte{[]byte("zadd"), []byte("test"), []byte("1.0"), []byte("k1")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("zadd"), []byte("test"), []byte("1.0"), []byte("k1"), []byte("1.1"), []byte("k2")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("zcount"), []byte("test"), []byte("1.0"), []byte("1.1")},
			resp.MakeIntData(2)},

		{[][]byte{[]byte("zcount"), []byte("test"), []byte("ff"), []byte("1.1")},
			resp.MakeErrorData("ERR value is not a valid float")},

		{[][]byte{[]byte("zcount"), []byte("test"), []byte("1.0"), []byte("ff")},
			resp.MakeErrorData("ERR value is not a valid float")},

		{[][]byte{[]byte("zcard"), []byte("test")},
			resp.MakeIntData(2)},

		{[][]byte{[]byte("zrem"), []byte("test"), []byte("test")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("zrem"), []byte("test"), []byte("k1")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("zincrby"), []byte("test"), []byte("1.0"), []byte("k2")},
			resp.MakeBulkData([]byte(fmt.Sprintf("%f", 2.1)))},

		{[][]byte{[]byte("zincrby"), []byte("test"), []byte("dsfsdf"), []byte("k2")},
			resp.MakeErrorData("ERR value is not a valid float")},

		{[][]byte{[]byte("zadd"), []byte("test"), []byte("1.0"), []byte("k1"), []byte("1.1"), []byte("k2")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("zrange"), []byte("test"), []byte("0"), []byte("-1")},
			resp.MakeArrayData([]resp.RedisData{resp.MakeBulkData([]byte("k1")), resp.MakeBulkData([]byte("k2"))})},

		{[][]byte{[]byte("zrange"), []byte("test"), []byte("fd"), []byte("-1")},
			resp.MakeErrorData("ERR value is not an integer or out of range")},

		{[][]byte{[]byte("zrange"), []byte("test"), []byte("0"), []byte("-1")},
			resp.MakeArrayData([]resp.RedisData{resp.MakeBulkData([]byte("k1")), resp.MakeBulkData([]byte("k2"))})},

		{[][]byte{[]byte("zrevrange"), []byte("test"), []byte("0"), []byte("-1")},
			resp.MakeArrayData([]resp.RedisData{resp.MakeBulkData([]byte("k2")), resp.MakeBulkData([]byte("k1"))})},

		{[][]byte{[]byte("zrevrange"), []byte("test"), []byte("fd"), []byte("-1")},
			resp.MakeErrorData("ERR value is not an integer or out of range")},

		{[][]byte{[]byte("zrevrange"), []byte("test"), []byte("0"), []byte("sfd")},
			resp.MakeErrorData("ERR value is not an integer or out of range")},

		{[][]byte{[]byte("zrank"), []byte("test"), []byte("k1")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("zrank"), []byte("test"), []byte("kg1")},
			resp.MakeStringData("nil")},

		{[][]byte{[]byte("zrevrank"), []byte("test"), []byte("k1")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("zrevrank"), []byte("test"), []byte("kg1")},
			resp.MakeStringData("nil")},

		{[][]byte{[]byte("zscore"), []byte("test"), []byte("k2")},
			resp.MakeStringData(fmt.Sprintf("%f", 2.1))},

		{[][]byte{[]byte("zrangebyscore"), []byte("test"), []byte("1.0"), []byte("2.5")},
			resp.MakeArrayData([]resp.RedisData{resp.MakeBulkData([]byte("k1")), resp.MakeBulkData([]byte("k2"))})},

		{[][]byte{[]byte("zrangebyscore"), []byte("test"), []byte("f"), []byte("2")},
			resp.MakeErrorData("ERR value is not a valid float")},

		{[][]byte{[]byte("zrangebyscore"), []byte("test"), []byte("2"), []byte("f")},
			resp.MakeErrorData("ERR value is not a valid float")},

		{[][]byte{[]byte("zrevrangebyscore"), []byte("test"), []byte("1.0"), []byte("2.5")},
			resp.MakeArrayData([]resp.RedisData{resp.MakeBulkData([]byte("k2")), resp.MakeBulkData([]byte("k1"))})},

		{[][]byte{[]byte("zrevrangebyscore"), []byte("test"), []byte("f"), []byte("2")},
			resp.MakeErrorData("ERR value is not a valid float")},

		{[][]byte{[]byte("zrevrangebyscore"), []byte("test"), []byte("1"), []byte("ff")},
			resp.MakeErrorData("ERR value is not a valid float")},

		{[][]byte{[]byte("zremrangebyrank"), []byte("test"), []byte("f"), []byte("2")},
			resp.MakeErrorData("ERR value is not an integer or out of range")},

		{[][]byte{[]byte("zremrangebyrank"), []byte("test"), []byte("1"), []byte("sfd")},
			resp.MakeErrorData("ERR value is not an integer or out of range")},

		{[][]byte{[]byte("zremrangebyrank"), []byte("test"), []byte("0"), []byte("2")},
			resp.MakeIntData(2)},

		{[][]byte{[]byte("zremrangebyscore"), []byte("test"), []byte("f"), []byte("2")},
			resp.MakeErrorData("ERR value is not a valid float")},

		{[][]byte{[]byte("zremrangebyscore"), []byte("test"), []byte("0"), []byte("f")},
			resp.MakeErrorData("ERR value is not a valid float")},

		{[][]byte{[]byte("zremrangebyscore"), []byte("test"), []byte("0"), []byte("2")},
			resp.MakeIntData(0)},
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
