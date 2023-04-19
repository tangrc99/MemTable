package server

import (
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/logger"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// TestAOFBufferPage 测试 page 能否正常写入
func TestAOFBufferPage(t *testing.T) {

	file, err := os.OpenFile("TestAOFBufferPage.aof", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	assert.Nil(t, err)
	t.Cleanup(func() {
		err = os.Remove("TestAOFBufferPage.aof")
	})
	page := newBufferPage(5)

	// 正常写入
	ret1 := page.append([]byte("12"))
	assert.Equal(t, appendSuccess, ret1)

	// 写入小内容，但是超出本页缓冲
	ret2 := page.append([]byte("1234"))
	assert.Equal(t, appendFail, ret2)

	// 写入一个大内容
	ret3 := page.append([]byte("345678"))
	assert.Equal(t, appendAppendix, ret3)

	// 在写入大内容后，写入小内容
	ret4 := page.append([]byte("90"))
	assert.Equal(t, appendAppendix, ret4)

	page.flush(file)

	bytes, err := os.ReadFile("TestAOFBufferPage.aof")

	assert.Equal(t, []byte("1234567890"), bytes)

	// 刷盘后，appendix 应该被清理
	ret5 := page.append([]byte("12"))
	assert.Equal(t, appendSuccess, ret5)
}

func TestAOFBuffer(t *testing.T) {

	file, err := os.OpenFile("TestAOFBuffer.aof", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	assert.Nil(t, err)

	t.Cleanup(func() {
		err = os.Remove("TestAOFBuffer.aof")
	})

	aof := &aofBuffer{
		writer:       file,
		pages:        make([]*bufferPage, 2),
		flushSeq:     0,
		appendSeq:    0,
		pageSize:     3,
		writing:      0,
		notification: make(chan struct{}),
		quitFlag:     make(chan struct{}),
	}
	for i := range aof.pages {
		aof.pages[i] = newBufferPage(5)
	}

	aof.append([]byte("12"))
	aof.append([]byte("345678"))
	aof.append([]byte("90"))

	{
		aof.flushBuffer()
		bytes, _ := os.ReadFile("TestAOFBuffer.aof")
		assert.Equal(t, []byte("12345678"), bytes)
	}

	{
		aof.flushBuffer()
		bytes, _ := os.ReadFile("TestAOFBuffer.aof")
		assert.Equal(t, []byte("1234567890"), bytes)
	}
}

func TestAOFBufferAsync(t *testing.T) {

	file, err := os.OpenFile("TestAOFBufferAsync.aof", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	assert.Nil(t, err)

	t.Cleanup(func() {
		err = os.Remove("TestAOFBufferAsync.aof")
	})
	aof := &aofBuffer{
		writer:       file,
		pages:        make([]*bufferPage, 2),
		flushSeq:     0,
		appendSeq:    0,
		pageSize:     3,
		writing:      0,
		notification: make(chan struct{}),
		quitFlag:     make(chan struct{}),
	}
	for i := range aof.pages {
		aof.pages[i] = newBufferPage(5)
	}

	go func() {
		aof.asyncTask()
	}()

	aof.append([]byte("12"))
	aof.append([]byte("345678"))
	aof.append([]byte("90"))

	{
		aof.flush()
		for atomic.LoadInt32(&aof.writing) > 0 {
		}

		bytes, _ := os.ReadFile("TestAOFBufferAsync.aof")
		assert.Equal(t, []byte("12345678"), bytes)
	}

	// 等待一段时间直到 AOF 缓冲区全部写入到硬盘中
	time.Sleep(time.Second)

	{
		aof.flush()
		for atomic.LoadInt32(&aof.writing) > 0 {
		}

		bytes, _ := os.ReadFile("TestAOFBufferAsync.aof")
		assert.Equal(t, []byte("1234567890"), bytes)
	}
	aof.quitFlag <- struct{}{}
}

func TestAOFBufferAsyncQuit(t *testing.T) {

	_ = logger.Init("", "", logger.WARNING)

	aof := newAOFBuffer("TestAOFBufferAsyncQuit.aof")

	t.Cleanup(func() {
		_ = os.Remove("TestAOFBufferAsyncQuit.aof")
	})

	aof.append([]byte("12"))
	aof.append([]byte("345678"))
	aof.append([]byte("90"))

	aof.quit()
	bytes, _ := os.ReadFile("TestAOFBufferAsyncQuit.aof")
	assert.Equal(t, []byte("1234567890"), bytes)
}
