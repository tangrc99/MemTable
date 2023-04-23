package cmd

import (
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/db/structure"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/global"
	"testing"
)

func TestCmdSet(t *testing.T) {
	database := db.NewDataBase(1)
	database.SetKey("s", structure.NewSet())
	tests := []struct {
		input    [][]byte
		expected resp.RedisData
	}{
		{[][]byte{[]byte("sadd"), []byte("test"), []byte("k1"), []byte("k2")},
			resp.MakeIntData(2)},

		{[][]byte{[]byte("srem"), []byte("test"), []byte("k3")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("srem"), []byte("test"), []byte("k2")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("sismember"), []byte("test"), []byte("k1")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("sismember"), []byte("test"), []byte("k2")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("sadd"), []byte("test"), []byte("k1"), []byte("k2")},
			resp.MakeIntData(1)},

		{[][]byte{[]byte("scard"), []byte("test")},
			resp.MakeIntData(2)},

		{[][]byte{[]byte("smove"), []byte("test"), []byte("k1"), []byte("k2")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("smove"), []byte("test"), []byte("k1"), []byte("k3")},
			resp.MakeIntData(0)},

		{[][]byte{[]byte("smove"), []byte("test"), []byte("s"), []byte("k1")},
			resp.MakeIntData(1)},
	}

	for _, test := range tests {
		cmd, exist := global.FindCommand(string(test.input[0]))
		assert.True(t, exist)
		c := cmd.Function().(command)

		ret := c(database, test.input)
		assert.Equal(t, test.expected, ret)
	}
}

func TestCmdSetRand(t *testing.T) {
	database := db.NewDataBase(1)
	set := structure.NewSet()
	set.Add("k1")
	set.Add("k2")
	set.Add("k3")
	set.Add("k4")

	database.SetKey("test", set)

	keys := []resp.RedisData{
		resp.MakeBulkData([]byte("k1")),
		resp.MakeBulkData([]byte("k2")),
		resp.MakeBulkData([]byte("k3")),
		resp.MakeBulkData([]byte("k4")),
	}

	tests := []struct {
		input    [][]byte
		expected int
	}{
		{[][]byte{[]byte("smembers"), []byte("test")},
			4},

		{[][]byte{[]byte("spop"), []byte("test"), []byte("2")},
			2},

		{[][]byte{[]byte("smembers"), []byte("test")},
			2},

		{[][]byte{[]byte("srandmember"), []byte("test"), []byte("2")},
			2},
	}

	for _, test := range tests {
		cmd, exist := global.FindCommand(string(test.input[0]))
		assert.True(t, exist)
		c := cmd.Function().(command)

		ret := c(database, test.input)
		assert.Equal(t, test.expected, len(ret.(*resp.ArrayData).Data()))
		assert.Subset(t, keys, ret.(*resp.ArrayData).Data())
	}
}

func TestCmdMultiSet(t *testing.T) {
	database := db.NewDataBase(1)
	set1 := structure.NewSet()
	set1.Add("k1")
	set1.Add("k2")
	set1.Add("k3")
	database.SetKey("set1", set1)
	set2 := structure.NewSet()
	set2.Add("k3")
	set2.Add("k4")
	database.SetKey("set2", set2)

	tests := []struct {
		input    [][]byte
		expected []resp.RedisData
	}{
		{[][]byte{[]byte("sdiff"), []byte("set1"), []byte("set2")},
			[]resp.RedisData{resp.MakeBulkData([]byte("k1")), resp.MakeBulkData([]byte("k2"))}},

		{[][]byte{[]byte("sinter"), []byte("set1"), []byte("set2")},
			[]resp.RedisData{resp.MakeBulkData([]byte("k3"))}},

		{[][]byte{[]byte("sunion"), []byte("set1"), []byte("set2")},
			[]resp.RedisData{resp.MakeBulkData([]byte("k1")), resp.MakeBulkData([]byte("k2")), resp.MakeBulkData([]byte("k3")), resp.MakeBulkData([]byte("k4"))}},
		//{[][]byte{[]byte("smove"), []byte("set1"), []byte("set2"), []byte("k2")},
		//	resp.MakeIntData(1)},
	}

	for _, test := range tests {
		cmd, exist := global.FindCommand(string(test.input[0]))
		assert.True(t, exist)
		c := cmd.Function().(command)

		ret := c(database, test.input)
		assert.Subset(t, test.expected, ret.(*resp.ArrayData).Data())
		assert.Equal(t, len(test.expected), len(ret.(*resp.ArrayData).Data()))
	}
}

func TestCmdMultiSetStore(t *testing.T) {
	database := db.NewDataBase(1)
	set1 := structure.NewSet()
	set1.Add("k1")
	set1.Add("k2")
	set1.Add("k3")
	database.SetKey("set1", set1)
	set2 := structure.NewSet()
	set2.Add("k3")
	set2.Add("k4")
	database.SetKey("set2", set2)

	tests := []struct {
		input    [][]byte
		expected []resp.RedisData
	}{
		{[][]byte{[]byte("sdiffstore"), []byte("set3"), []byte("set1"), []byte("set2")},
			[]resp.RedisData{resp.MakeBulkData([]byte("k1")), resp.MakeBulkData([]byte("k2"))}},

		{[][]byte{[]byte("sdiffstore"), []byte("set3"), []byte("set1")},
			[]resp.RedisData{resp.MakeBulkData([]byte("k1")), resp.MakeBulkData([]byte("k2")), resp.MakeBulkData([]byte("k3"))}},

		{[][]byte{[]byte("sinterstore"), []byte("set3"), []byte("set1"), []byte("set2")},
			[]resp.RedisData{resp.MakeBulkData([]byte("k3"))}},

		{[][]byte{[]byte("sinterstore"), []byte("set3"), []byte("set1")},
			[]resp.RedisData{resp.MakeBulkData([]byte("k1")), resp.MakeBulkData([]byte("k2")), resp.MakeBulkData([]byte("k3"))}},

		{[][]byte{[]byte("sunionstore"), []byte("set3"), []byte("set1"), []byte("set2")},
			[]resp.RedisData{resp.MakeBulkData([]byte("k1")), resp.MakeBulkData([]byte("k2")), resp.MakeBulkData([]byte("k3")), resp.MakeBulkData([]byte("k4"))}},
		//{[][]byte{[]byte("smove"), []byte("set1"), []byte("set2"), []byte("k2")},
		//	resp.MakeIntData(1)},
	}

	for _, test := range tests {
		cmd, exist := global.FindCommand(string(test.input[0]))
		assert.True(t, exist)
		c := cmd.Function().(command)

		ret := c(database, test.input)
		assert.Equal(t, resp.MakeIntData(int64(len(test.expected))), ret)

		ret = sMembers(database, [][]byte{[]byte("smembers"), []byte("set3")})

		assert.Subset(t, test.expected, ret.(*resp.ArrayData).Data())
		assert.Equal(t, len(test.expected), len(ret.(*resp.ArrayData).Data()))

		database.DeleteKey("set3")
	}
}
