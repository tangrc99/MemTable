package resp

import (
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/logger"
	"os"
	"testing"
)

func TestRespBasic(t *testing.T) {
	rd, wr, err := os.Pipe()
	assert.Nil(t, err)

	parser := NewParser(rd)

	msg1 := "*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	n, err := wr.WriteString(msg1)
	assert.Nil(t, err)
	assert.Equal(t, len(msg1), n)

	ret := parser.Parse()
	assert.Nil(t, ret.Err)
	assert.False(t, ret.Abort)
	assert.Equal(t, []byte("*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"), ret.Data.ToBytes())
	assert.Equal(t, [][]byte{[]byte("set"), []byte("key"), []byte("value")}, ret.Data.(*ArrayData).ToCommand())
}

func TestRespBasic2(t *testing.T) {
	rd, wr, err := os.Pipe()
	assert.Nil(t, err)

	parser := NewParser(rd)

	msg1 := "*4\r\n$3\r\nset\r\n:1\r\n+value\r\n-err\r\n"
	n, err := wr.WriteString(msg1)
	assert.Nil(t, err)
	assert.Equal(t, len(msg1), n)

	ret := parser.Parse()
	assert.Nil(t, ret.Err)
	assert.False(t, ret.Abort)
	assert.Equal(t, []byte(msg1), ret.Data.ToBytes())
	assert.Equal(t, [][]byte{[]byte("set"), []byte("1"), []byte("value"), []byte("err")}, ret.Data.(*ArrayData).ToCommand())
}

// TestRespReadBroken 读取一个关闭的 fd 应该报错
func TestRespReadBroken(t *testing.T) {
	rd, wr, err := os.Pipe()
	assert.Nil(t, err)

	parser := NewParser(rd)
	wr.Close()

	ret := parser.Parse()
	assert.NotNil(t, ret.Err)
}

func TestRespStop(t *testing.T) {
	rd, wr, err := os.Pipe()
	assert.Nil(t, err)

	parser := NewParser(rd)
	wr.Close()

	parser.Stop()
	ret := parser.Parse()
	assert.Nil(t, ret.Err)
	assert.True(t, ret.Abort)
}

func TestRespPipeline(t *testing.T) {

	rd, wr, err := os.Pipe()
	assert.Nil(t, err)

	parser := NewParser(rd)

	pipelined := "set 111 222\r\nget 111\r\nping\r\nping\r\n"

	n, err := wr.WriteString(pipelined)
	assert.Nil(t, err)
	assert.Equal(t, len(pipelined), n)

	ret1 := parser.Parse()
	assert.Nil(t, ret1.Err)
	assert.IsType(t, &PlainData{}, ret1.Data)
	assert.Equal(t, []byte("set 111 222\r\n"), ret1.Data.ToBytes())
	assert.Equal(t, [][]byte{[]byte("set"), []byte("111"), []byte("222")}, ret1.Data.(*PlainData).ToCommand())

	ret2 := parser.Parse()
	assert.Nil(t, ret2.Err)
	assert.IsType(t, &PlainData{}, ret2.Data)
	assert.Equal(t, []byte("get 111\r\n"), ret2.Data.ToBytes())
	assert.Equal(t, [][]byte{[]byte("get"), []byte("111")}, ret2.Data.(*PlainData).ToCommand())

	ret3 := parser.Parse()
	assert.Nil(t, ret3.Err)
	assert.IsType(t, &PlainData{}, ret3.Data)
	assert.Equal(t, []byte("ping\r\n"), ret3.Data.ToBytes())
	assert.Equal(t, [][]byte{[]byte("ping")}, ret3.Data.(*PlainData).ToCommand())

	ret4 := parser.Parse()
	assert.Nil(t, ret4.Err)
	assert.IsType(t, &PlainData{}, ret4.Data)
	assert.Equal(t, []byte("ping\r\n"), ret4.Data.ToBytes())
	assert.Equal(t, [][]byte{[]byte("ping")}, ret4.Data.(*PlainData).ToCommand())

	wr.Close()

	ret5 := parser.Parse()
	assert.NotNil(t, ret5.Err)
}

func TestRespError1(t *testing.T) {

	_ = logger.Init("", "", logger.PANIC)

	rd, wr, err := os.Pipe()
	assert.Nil(t, err)

	parser := NewParser(rd)

	msg1 := "*3\r\n$3\r\ns\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	n, err := wr.WriteString(msg1)
	assert.Nil(t, err)
	assert.Equal(t, len(msg1), n)

	ret := parser.Parse()
	assert.NotNil(t, ret.Err)
	assert.False(t, ret.Abort)

}

func TestRespError2(t *testing.T) {

	_ = logger.Init("", "", logger.PANIC)

	rd, wr, err := os.Pipe()
	assert.Nil(t, err)

	parser := NewParser(rd)

	msg1 := "*2\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	n, err := wr.WriteString(msg1)
	assert.Nil(t, err)
	assert.Equal(t, len(msg1), n)

	ret := parser.Parse()
	assert.Nil(t, ret.Err)
	assert.False(t, ret.Abort)

}

func TestRespError3(t *testing.T) {

	_ = logger.Init("", "", logger.PANIC)

	rd, wr, err := os.Pipe()
	assert.Nil(t, err)

	parser := NewParser(rd)

	msg1 := "*3\r\n$3\r\nset\r\n$3\rkey\r\n$5\r\nvalue\r\n"
	n, err := wr.WriteString(msg1)
	assert.Nil(t, err)
	assert.Equal(t, len(msg1), n)

	ret := parser.Parse()
	assert.NotNil(t, ret.Err)
	assert.False(t, ret.Abort)

}

func TestRespError4(t *testing.T) {

	_ = logger.Init("", "", logger.PANIC)

	rd, wr, err := os.Pipe()
	assert.Nil(t, err)

	parser := NewParser(rd)

	msg1 := "*3\r\n$3\r\nset\r\n$3\r\nkey44\r\n$5\r\nvalue\r\n"
	n, err := wr.WriteString(msg1)
	assert.Nil(t, err)
	assert.Equal(t, len(msg1), n)

	ret := parser.Parse()
	assert.NotNil(t, ret.Err)
	assert.False(t, ret.Abort)

}

func TestRespError5(t *testing.T) {

	_ = logger.Init("", "", logger.PANIC)

	rd, wr, err := os.Pipe()
	assert.Nil(t, err)

	parser := NewParser(rd)

	// $ 后面不是数字
	msg1 := "*3\r\n$3\r\nset\r\n$f\r\nkey\r\n$5\r\nvalue\r\n"
	n, err := wr.WriteString(msg1)
	assert.Nil(t, err)
	assert.Equal(t, len(msg1), n)

	ret := parser.Parse()
	assert.NotNil(t, ret.Err)
	assert.False(t, ret.Abort)

}

func TestRespError6(t *testing.T) {

	_ = logger.Init("", "", logger.PANIC)

	rd, wr, err := os.Pipe()
	assert.Nil(t, err)

	parser := NewParser(rd)

	// * 后面不是数字
	msg1 := "*f\r\n$3\r\nset\r\n$f\r\nkey\r\n+value\r\n"
	n, err := wr.WriteString(msg1)
	assert.Nil(t, err)
	assert.Equal(t, len(msg1), n)

	ret := parser.Parse()
	assert.NotNil(t, ret.Err)
	assert.False(t, ret.Abort)

}

func TestRespError7(t *testing.T) {

	_ = logger.Init("", "", logger.PANIC)

	rd, wr, err := os.Pipe()
	assert.Nil(t, err)

	parser := NewParser(rd)

	msg1 := "f3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	n, err := wr.WriteString(msg1)
	assert.Nil(t, err)
	assert.Equal(t, len(msg1), n)

	ret1 := parser.Parse()
	assert.Nil(t, ret1.Err)
	assert.False(t, ret1.Abort)

	ret2 := parser.Parse()
	assert.Nil(t, ret2.Err)

}

func TestRespError8(t *testing.T) {

	_ = logger.Init("", "", logger.PANIC)

	rd, wr, err := os.Pipe()
	assert.Nil(t, err)

	parser := NewParser(rd)

	msg1 := "*-1\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	n, err := wr.WriteString(msg1)
	assert.Nil(t, err)
	assert.Equal(t, len(msg1), n)

	ret1 := parser.Parse()
	assert.Nil(t, ret1.Err)
	assert.False(t, ret1.Abort)

	msg2 := "*0\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	n2, err := wr.WriteString(msg2)
	assert.Nil(t, err)
	assert.Equal(t, len(msg2), n2)

	ret2 := parser.Parse()
	assert.Nil(t, ret2.Err)
	assert.False(t, ret2.Abort)
}

func TestRespError9(t *testing.T) {

	_ = logger.Init("", "", logger.PANIC)

	rd, wr, err := os.Pipe()
	assert.Nil(t, err)

	parser := NewParser(rd)

	msg1 := "*0\r\n$-1\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	n, err := wr.WriteString(msg1)
	assert.Nil(t, err)
	assert.Equal(t, len(msg1), n)

	ret1 := parser.Parse()
	assert.Nil(t, ret1.Err)
	assert.False(t, ret1.Abort)

}
