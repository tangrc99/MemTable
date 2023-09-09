package server

import (
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/global"
	"testing"
)

func TestCmdConn(t *testing.T) {
	s := NewServer()
	cli := NewFakeClient()

	tests := []struct {
		input    [][]byte
		expected resp.RedisData
		f        func()
	}{
		{[][]byte{[]byte("ping")},
			resp.MakeStringData("pong"),
			func() {},
		},

		{[][]byte{[]byte("quit")},
			nil,
			func() {
				assert.Zero(t, s.clis.Size())
			},
		},

		{[][]byte{[]byte("select")},
			resp.MakeErrorData("ERR wrong number of arguments for 'select' command"),
			func() {},
		},

		{[][]byte{[]byte("select"), []byte("1000")},
			resp.MakeErrorData("ERR DB index is out of range"),
			func() {},
		},

		{[][]byte{[]byte("select"), []byte("ff")},
			resp.MakeErrorData("ERR value is not an integer or out of range"),
			func() {},
		},

		{[][]byte{[]byte("select"), []byte("2")},
			resp.MakeStringData("OK"),
			func() {
				assert.Equal(t, 2, cli.dbSeq)
			},
		},
	}

	for _, test := range tests {
		cmd, exist := global.FindCommand(string(test.input[0]))
		assert.True(t, exist)
		c := cmd.Function().(Command)

		ret := c(s, cli, test.input)

		assert.Equal(t, test.expected, ret)
		test.f()
	}
}

func TestCmdPubSub(t *testing.T) {
	s := NewServer()
	cli := NewFakeClient()
	cli1 := NewFakeClient()

	tests := []struct {
		input    [][]byte
		expected resp.RedisData
		client   *Client
	}{
		{[][]byte{[]byte("publish"), []byte("ch1"), []byte("msg")},
			resp.MakeIntData(0),
			cli,
		},

		{[][]byte{[]byte("subscribe"), []byte("ch1")},
			resp.MakeArrayData([]resp.RedisData{
				resp.MakeIntData(1),
				resp.MakeBulkData([]byte("subscribe")),
				resp.MakeBulkData([]byte("ch1")),
			}),
			cli1,
		},

		{[][]byte{[]byte("publish"), []byte("ch1"), []byte("msg")},
			resp.MakeIntData(1),
			cli,
		},

		{[][]byte{[]byte("unsubscribe"), []byte("ch1")},
			resp.MakeArrayData([]resp.RedisData{
				resp.MakeIntData(0),
				resp.MakeBulkData([]byte("unsubscribe")),
				resp.MakeBulkData([]byte("ch1")),
			}),
			cli1,
		},

		{[][]byte{[]byte("publish"), []byte("ch1"), []byte("msg")},
			resp.MakeIntData(0),
			cli,
		},
	}

	for _, test := range tests {
		cmd, exist := global.FindCommand(string(test.input[0]))
		assert.True(t, exist)
		c := cmd.Function().(Command)

		ret := c(s, test.client, test.input)

		assert.Equal(t, test.expected, ret)
	}
}

func TestCmdTX(t *testing.T) {

	s := NewServer()
	cli := NewFakeClient()

	tests := []struct {
		input    [][]byte
		expected resp.RedisData
		f        func()
	}{
		{[][]byte{[]byte("exec")},
			resp.MakeErrorData("ERR EXEC without MULTI"),
			func() {},
		},

		{[][]byte{[]byte("discard")},
			resp.MakeErrorData("ERR DISCARD without MULTI"),
			func() {},
		},

		{[][]byte{[]byte("multi")},
			resp.MakeStringData("OK"),
			func() {},
		},

		{[][]byte{[]byte("multi")},
			resp.MakeErrorData("ERR MULTI calls can not be nested"),
			func() {},
		},

		{[][]byte{[]byte("watch"), []byte("k1")},
			resp.MakeErrorData("ERR WATCH inside MULTI is not allowed"),
			func() {},
		},

		{[][]byte{[]byte("discard")},
			resp.MakeStringData("OK"),
			func() {},
		},

		{[][]byte{[]byte("watch"), []byte("k1")},
			resp.MakeStringData("OK"),
			func() {
				s.dbs[cli.dbSeq].ReviseNotifyAll()
			},
		},

		{[][]byte{[]byte("multi")},
			resp.MakeStringData("OK"),
			func() {},
		},

		{[][]byte{[]byte("exec")},
			resp.MakeStringData("nil"),
			func() {},
		},

		{[][]byte{[]byte("watch"), []byte("k1")},
			resp.MakeStringData("OK"),
			func() {},
		},

		{[][]byte{[]byte("multi")},
			resp.MakeStringData("OK"),
			func() {},
		},

		{[][]byte{[]byte("exec")},
			resp.MakeEmptyArrayData(),
			func() {},
		},

		{[][]byte{[]byte("watch"), []byte("k1")},
			resp.MakeStringData("OK"),
			func() {},
		},

		{[][]byte{[]byte("multi")},
			resp.MakeStringData("OK"),
			func() {
				cli.tx = append(cli.tx, [][]byte{[]byte("set"), []byte("k"), []byte("v")})
			},
		},

		{[][]byte{[]byte("exec")},
			resp.MakeArrayData([]resp.RedisData{resp.MakeStringData("OK")}),
			func() {},
		},
	}

	for _, test := range tests {
		cmd, exist := global.FindCommand(string(test.input[0]))
		assert.True(t, exist)
		c := cmd.Function().(Command)

		ret := c(s, cli, test.input)

		assert.Equal(t, test.expected, ret)
		test.f()
	}

}
