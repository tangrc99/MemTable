package cmd

import (
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/global"
	"testing"
)

func TestCmdBloomFilter(t *testing.T) {
	database := db.NewDataBase(1)

	tests := []struct {
		input    [][]byte
		expected resp.RedisData
	}{
		{[][]byte{[]byte("bf.add"), []byte("test"), []byte("k1")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("bf.add"), []byte("test"), []byte("k1")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("bf.madd"), []byte("test"), []byte("k1"), []byte("k2"), []byte("k3"), []byte("k4")},
			resp.MakeIntData(3)},

		{[][]byte{[]byte("bf.exists"), []byte("test"), []byte("k1")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("bf.exists"), []byte("test"), []byte("k6")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("bf.exists"), []byte("test11"), []byte("k6")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("bf.mexists"), []byte("test"), []byte("k1"), []byte("k2"), []byte("k3"), []byte("k4")},
			resp.MakeIntData(4)},

		{[][]byte{[]byte("bf.info"), []byte("test")},
			resp.MakeArrayData([]resp.RedisData{
				resp.MakeBulkData([]byte("Capacity")),
				resp.MakeIntData(16383),
				resp.MakeBulkData([]byte("Size")),
				resp.MakeIntData(2088),
				resp.MakeBulkData([]byte("Number of filters")),
				resp.MakeIntData(3),
				resp.MakeBulkData([]byte("Number of items inserted")),
				resp.MakeIntData(4),
			})},

		{[][]byte{[]byte("bf.reserve"), []byte("test"), []byte("1"), []byte("2")},
			resp.MakeErrorData("ERR item exists")},

		{[][]byte{[]byte("bf.reserve"), []byte("b"), []byte("f"), []byte("2")},
			resp.MakeErrorData("ERR error_rate is out of range")},

		{[][]byte{[]byte("bf.reserve"), []byte("b"), []byte("1"), []byte("f")},
			resp.MakeErrorData("ERR capacity is out of range")},

		{[][]byte{[]byte("bf.reserve"), []byte("b"), []byte("0.01"), []byte("1000")},
			resp.MakeStringData("OK")},
	}

	for _, test := range tests {
		cmd, exist := global.FindCommand(string(test.input[0]))
		assert.True(t, exist)
		c := cmd.Function().(command)

		ret := c(database, test.input)
		assert.Equal(t, test.expected, ret)
	}
}
