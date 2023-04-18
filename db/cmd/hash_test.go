package cmd

import (
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/db/structure"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/global"
	"testing"
)

func TestCmdHash(t *testing.T) {

	database := db.NewDataBase(1)
	dict := structure.NewDict(1)
	database.SetKey("test", dict)

	tests := []struct {
		input    [][]byte
		expected resp.RedisData
	}{
		{[][]byte{[]byte("hset"), []byte("test"), []byte("key"), []byte("value")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("hset"), []byte("test"), []byte("key")},
			resp.MakeErrorData("ERR wrong number of arguments for 'hset' command")},

		{[][]byte{[]byte("hexists"), []byte("n"), []byte("key")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("hget"), []byte("test"), []byte("key")},
			resp.MakeBulkData([]byte("value"))},

		{[][]byte{[]byte("hget"), []byte("test"), []byte("n")},
			resp.MakeStringData("nil")},

		{[][]byte{[]byte("hstrlen"), []byte("test"), []byte("key")},
			resp.MakeIntData(5)},

		{[][]byte{[]byte("hstrlen"), []byte("test"), []byte("n")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("hlen"), []byte("test")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("hmset"), []byte("test"), []byte("k1"), []byte("v1"), []byte("n"), []byte("1")},
			resp.MakeStringData("OK")},

		{[][]byte{[]byte("hmset"), []byte("test"), []byte("k1"), []byte("v1"), []byte("n")},
			resp.MakeErrorData("ERR wrong number of arguments for 'hmset' command")},

		{[][]byte{[]byte("hdel"), []byte("test"), []byte("key")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("hdel"), []byte("test"), []byte("sfdfsd")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("hincrby"), []byte("test"), []byte("n")},
			resp.MakeErrorData("ERR wrong number of arguments for 'hincrby' command")},

		{[][]byte{[]byte("hincrby"), []byte("test"), []byte("n"), []byte("1")},
			resp.MakeIntData(2)},

		{[][]byte{[]byte("hincrby"), []byte("test"), []byte("n"), []byte("sdf")},
			resp.MakeErrorData("ERR value is not an integer or out of range")},

		{[][]byte{[]byte("hincrby"), []byte("test"), []byte("k1"), []byte("1")},
			resp.MakeErrorData("ERR hash value is not an integer")},

		{[][]byte{[]byte("hincrby"), []byte("test"), []byte("n1"), []byte("1")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("hmget"), []byte("test"), []byte("n1"), []byte("k1")},
			resp.MakeArrayData([]resp.RedisData{resp.MakeBulkData([]byte("1")), resp.MakeBulkData([]byte("v1"))})},
	}

	for _, test := range tests {
		cmd, exist := global.FindCommand(string(test.input[0]))
		assert.True(t, exist)
		c := cmd.Function().(command)

		ret := c(database, test.input)
		assert.Equal(t, test.expected, ret)
	}

}

func TestCmdHashRand(t *testing.T) {
	database := db.NewDataBase(1)
	dict := structure.NewDict(1)
	database.SetKey("test", dict)
	dict.Set("k1", Slice("v1"))
	dict.Set("k2", Slice("v2"))
	dict.Set("k3", Slice("v3"))
	dict.Set("k4", Slice("v4"))

	keys := []resp.RedisData{resp.MakeBulkData([]byte("k1")),
		resp.MakeBulkData([]byte("k2")),
		resp.MakeBulkData([]byte("k3")),
		resp.MakeBulkData([]byte("k4")),
	}

	tests := []struct {
		input    [][]byte
		expected []resp.RedisData
	}{
		{[][]byte{[]byte("hrandfield"), []byte("test")},
			keys},

		{[][]byte{[]byte("hrandfield"), []byte("test"), []byte("2")},
			keys},
	}

	for _, test := range tests {
		cmd, exist := global.FindCommand(string(test.input[0]))
		assert.True(t, exist)
		c := cmd.Function().(command)

		ret := c(database, test.input)
		d := ret.(*resp.ArrayData).Data()
		assert.Subset(t, test.expected, d)
	}
}

func TestCmdHashAll(t *testing.T) {
	database := db.NewDataBase(1)
	dict := structure.NewDict(1)
	database.SetKey("test", dict)
	dict.Set("k1", Slice("v1"))
	dict.Set("k2", Slice("v2"))
	dict.Set("k3", Slice("v3"))
	dict.Set("k4", Slice("v4"))

	tests := []struct {
		input    [][]byte
		expected []resp.RedisData
	}{
		{[][]byte{[]byte("hkeys"), []byte("test")},
			[]resp.RedisData{resp.MakeBulkData([]byte("k1")), resp.MakeBulkData([]byte("k2")), resp.MakeBulkData([]byte("k3")), resp.MakeBulkData([]byte("k4"))}},

		{[][]byte{[]byte("hvals"), []byte("test")},
			[]resp.RedisData{resp.MakeBulkData([]byte("v1")), resp.MakeBulkData([]byte("v2")), resp.MakeBulkData([]byte("v3")), resp.MakeBulkData([]byte("v4"))}},

		{[][]byte{[]byte("hgetall"), []byte("test")},
			[]resp.RedisData{resp.MakeBulkData([]byte("v1")), resp.MakeBulkData([]byte("v2")), resp.MakeBulkData([]byte("v3")), resp.MakeBulkData([]byte("v4")),
				resp.MakeBulkData([]byte("k1")), resp.MakeBulkData([]byte("k2")), resp.MakeBulkData([]byte("k3")), resp.MakeBulkData([]byte("k4"))}},
	}

	for _, test := range tests {
		cmd, exist := global.FindCommand(string(test.input[0]))
		assert.True(t, exist)
		c := cmd.Function().(command)

		ret := c(database, test.input)
		d := ret.(*resp.ArrayData).Data()
		assert.Subset(t, test.expected, d)
		assert.Equal(t, len(test.expected), len(d))
	}

}
