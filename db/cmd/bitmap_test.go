package cmd

import (
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/global"
	"testing"
)

func TestCmdBitmap(t *testing.T) {
	database := db.NewDataBase(1)

	tests := []struct {
		input    [][]byte
		expected resp.RedisData
	}{
		{[][]byte{[]byte("setbit"), []byte("test"), []byte("key"), []byte("1")},
			resp.MakeErrorData("ERR bit offset is not an integer or out of range")},

		{[][]byte{[]byte("setbit"), []byte("test"), []byte("1"), []byte("f")},
			resp.MakeErrorData("ERR bit is not an integer or out of range")},

		{[][]byte{[]byte("setbit"), []byte("test"), []byte("10"), []byte("1")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("getbit"), []byte("test"), []byte("key")},
			resp.MakeErrorData("ERR bit offset is not an integer or out of range")},

		{[][]byte{[]byte("getbit"), []byte("test"), []byte("10")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("setbit"), []byte("test"), []byte("10"), []byte("0")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("setbit"), []byte("test"), []byte("11"), []byte("1")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("setbit"), []byte("test"), []byte("10"), []byte("1")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("setbit"), []byte("test"), []byte("3"), []byte("1")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("bitcount"), []byte("test")},
			resp.MakeIntData(3)},

		{[][]byte{[]byte("bitcount"), []byte("test"), []byte("0"), []byte("0")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("bitcount"), []byte("test"), []byte("1"), []byte("1")},
			resp.MakeIntData(2)},

		{[][]byte{[]byte("bitcount"), []byte("test"), []byte("10"), []byte("0")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("bitcount"), []byte("test"), []byte("f"), []byte("0")},
			resp.MakeErrorData("ERR start is not an integer or out of range")},

		{[][]byte{[]byte("bitcount"), []byte("test"), []byte("10"), []byte("f")},
			resp.MakeErrorData("ERR end is not an integer or out of range")},

		{[][]byte{[]byte("bitpos"), []byte("test"), []byte("0")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("bitpos"), []byte("test"), []byte("1")},
			resp.MakeIntData(4)},

		{[][]byte{[]byte("bitpos"), []byte("test"), []byte("1"), []byte("1"), []byte("1")},
			resp.MakeIntData(12)},

		{[][]byte{[]byte("bitpos"), []byte("test"), []byte("f"), []byte("1"), []byte("1")},
			resp.MakeErrorData("ERR bit offset is not an integer or out of range")},

		{[][]byte{[]byte("bitpos"), []byte("test"), []byte("1"), []byte("f"), []byte("1")},
			resp.MakeErrorData("ERR start is not an integer or out of range")},

		{[][]byte{[]byte("bitpos"), []byte("test"), []byte("1"), []byte("1"), []byte("f")},
			resp.MakeErrorData("ERR end is not an integer or out of range")},
	}

	for _, test := range tests {
		cmd, exist := global.FindCommand(string(test.input[0]))
		assert.True(t, exist)
		c := cmd.Function().(command)

		ret := c(database, test.input)
		assert.Equal(t, test.expected, ret)
	}
}
