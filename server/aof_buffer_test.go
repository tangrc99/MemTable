package server

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/logger"
	"os"
	"sync/atomic"
	"testing"
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

	content, err := os.ReadFile("TestAOFBufferPage.aof")

	assert.Equal(t, []byte("1234567890"), content)

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
		content, _ := os.ReadFile("TestAOFBuffer.aof")
		assert.Equal(t, []byte("12345678"), content)
	}

	{
		aof.flushBuffer()
		content, _ := os.ReadFile("TestAOFBuffer.aof")
		assert.Equal(t, []byte("1234567890"), content)
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

		content, _ := os.ReadFile("TestAOFBufferAsync.aof")
		assert.Equal(t, []byte("12345678"), content)
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
	content, _ := os.ReadFile("TestAOFBufferAsyncQuit.aof")
	assert.Equal(t, []byte("1234567890"), content)
}

// TestAOFBufferCompetition 测试异步写入时的临界区竞争是否有问题
func TestAOFBufferCompetition(t *testing.T) {
	_ = logger.Init("", "", logger.WARNING)
	logger.Disable()

	t.Cleanup(func() {
		_ = os.Remove("TestAOFBufferCompetition.aof")
	})

	for i := 0; i < 100; i++ {

		file, err := os.OpenFile("TestAOFBufferCompetition.aof", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		assert.Nil(t, err)

		aof := &aofBuffer{
			writer:       file,
			pages:        make([]*bufferPage, 3),
			flushSeq:     0,
			appendSeq:    0,
			pageSize:     3,
			writing:      0,
			notification: make(chan struct{}),
			quitFlag:     make(chan struct{}),
		}
		for p := range aof.pages {
			aof.pages[p] = newBufferPage(5)
		}

		go func() {
			aof.asyncTask()
		}()

		aof.append(bytes.Repeat([]byte{'1'}, 5))
		aof.append(bytes.Repeat([]byte{'2'}, 5))
		aof.append(bytes.Repeat([]byte{'3'}, 5))
		// 这里可能会存在竞争
		aof.flush()
		aof.append(bytes.Repeat([]byte{'4'}, 5))

		aof.quit()

		bb, _ := os.ReadFile("TestAOFBufferCompetition.aof")
		assert.Equal(t, []byte("11111222223333344444"), bb)
		_ = os.Remove("TestAOFBufferCompetition.aof")

	}

}
