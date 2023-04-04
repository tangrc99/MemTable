package server

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/server/global"
	"testing"
)

func TestSlowLog(t *testing.T) {

	now := global.Now

	sl := newSlowLog(3)

	sl.appendEntry([][]byte{[]byte("set"), []byte("test"), []byte("value")}, 5)

	ret := sl.getEntries(1)

	assert.Equal(t, []byte(fmt.Sprintf("%d%d%dsettestvalue", 1, now.Unix(), 5)), ret.ByteData())

	sl.appendEntry([][]byte{[]byte("set"), []byte("test"), []byte("value")}, 5)
	sl.appendEntry([][]byte{[]byte("set"), []byte("test"), []byte("value")}, 5)
	sl.appendEntry([][]byte{[]byte("set"), []byte("test"), []byte("value")}, 5)
	ret = sl.getEntries(1)

	assert.Equal(t, []byte(fmt.Sprintf("%d%d%dsettestvalue", 2, now.Unix(), 5)), ret.ByteData())
}
