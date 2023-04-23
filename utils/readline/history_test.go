package readline

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHistoryBasic(t *testing.T) {

	h := newHistory(10)
	h.recordCommand([]byte("123"))

	c, end := h.moveCursor(true)
	assert.Equal(t, []byte("123"), c)
	assert.False(t, end)

	c, end = h.moveCursor(true)
	assert.Equal(t, []byte{}, c)
	assert.True(t, end)

	c, end = h.moveCursor(false)
	assert.Equal(t, []byte{}, c)
	assert.False(t, end)

	h.recordCommand([]byte("1234"))

	c, end = h.moveCursor(false)
	assert.Equal(t, []byte{}, c)
	assert.True(t, end)

	c, end = h.moveCursor(true)
	assert.Equal(t, []byte("1234"), c)
	assert.False(t, end)

	c, end = h.moveCursor(true)
	assert.Equal(t, []byte("123"), c)
	assert.False(t, end)

	c, end = h.moveCursor(false)
	assert.Equal(t, []byte("1234"), c)
	assert.False(t, end)
}

func TestHistorySearch(t *testing.T) {

	h := newHistory(10)
	h.recordCommand([]byte("456"))
	h.recordCommand([]byte("2354"))
	h.recordCommand([]byte("345654"))
	h.recordCommand([]byte("2345"))
	h.recordCommand([]byte("34"))
	h.recordCommand([]byte("wef"))
	h.recordCommand([]byte("sdc"))
	h.recordCommand([]byte("vdf5"))

	var searchRet []byte
	searchRet = h.searchCommand([]byte("5"))
	assert.Equal(t, []byte("vdf5"), searchRet)
	searchRet = h.searchCommand([]byte("5"))
	assert.Equal(t, []byte("2345"), searchRet)
	searchRet = h.searchCommand([]byte("5"))
	assert.Equal(t, []byte("345654"), searchRet)
	searchRet = h.searchCommand([]byte("5"))
	assert.Equal(t, []byte("2354"), searchRet)
	searchRet = h.searchCommand([]byte("5"))
	assert.Equal(t, []byte("456"), searchRet)
	searchRet = h.searchCommand([]byte("5"))
	assert.Equal(t, []byte{}, searchRet)

}
