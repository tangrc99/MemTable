package server

import (
	"github.com/tangrc99/MemTable/logger"
	"os"
	"sync/atomic"
)

// bufferPage 是一个单写缓冲区，如果需要多写需要加锁
type bufferPage struct {
	content []byte
	pos     int
	max     int
}

func newBufferPage(maxSize int) *bufferPage {
	return &bufferPage{
		content: make([]byte, maxSize),
		pos:     0,
		max:     maxSize,
	}
}

// append 返回失败代表当前 page 已经写满
func (buff *bufferPage) append(bytes []byte) bool {

	l := len(bytes)

	if l+buff.pos > buff.max {
		return false
	}

	copy(buff.content[buff.pos:], bytes)
	buff.pos += l
	return true
}

// flush 返回失败代表当前 page 有待写入内容
func (buff *bufferPage) flush(writer *os.File) bool {

	if writer == nil {
		return true
	}

	wn := 0

	for wn != buff.pos {

		w, err := writer.Write(buff.content[wn:buff.pos])
		if err != nil {
			logger.Error("Aof:", err.Error())
			os.Exit(-1)
		}
		wn += w
	}

	buff.pos = 0

	return true
}

type aofBuffer struct {
	writer *os.File

	flushSeq  int64 // 当前刷盘序列号
	appendSeq int64 // 当前写入序列号
	pages     []*bufferPage
	pageSize  int64

	writing      int32         // 是否正在写入
	notification chan struct{} // 刷盘通知标志
	quitFlag     chan struct{}
}

func newAOFBuffer(filename string) *aofBuffer {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		logger.Error("Aof:", err.Error())
	}

	buffers := &aofBuffer{
		writer:       file,
		pages:        make([]*bufferPage, 3),
		flushSeq:     0,
		appendSeq:    0,
		pageSize:     3,
		writing:      0,
		notification: make(chan struct{}),
		quitFlag:     make(chan struct{}),
	}
	buffers.pages[0] = newBufferPage(65536)
	buffers.pages[1] = newBufferPage(65536)
	buffers.pages[2] = newBufferPage(65536)

	go func() {

		q := false
		for !q {
			select {
			case <-buffers.notification:
				// 写入 os 缓冲区
				atomic.StoreInt32(&buffers.writing, 1)
				buffers.flushBuffer()

				// os 缓冲区写入硬盘
				atomic.StoreInt32(&buffers.writing, 2)
				buffers.syncToDisk()

				atomic.StoreInt32(&buffers.writing, 0)

			case <-buffers.quitFlag:
				q = true
			}
		}
		logger.Info("AOF: AOF Goroutine exits")
	}()

	return buffers
}

func (buff *aofBuffer) flushBuffer() {

	if buff.writer == nil {
		return
	}

	buff.pages[buff.flushSeq%buff.pageSize].flush(buff.writer)
	if buff.flushSeq == buff.appendSeq {
		return
	}
	buff.flushSeq++
}

func (buff *aofBuffer) syncToDisk() {

	err := buff.writer.Sync()
	if err != nil {
		return
	}
}

// quit 会阻塞并清空所有的缓冲区
func (buff *aofBuffer) quit() {

	if buff.writer == nil {
		return
	}

	for buff.flushSeq < buff.appendSeq {
		// 通知协程进行写入操作
		buff.notification <- struct{}{}
	}

	// 追上时也要刷盘一次
	buff.notification <- struct{}{}

	buff.quitFlag <- struct{}{}
}

// flush 通知协程进行持久化操作
func (buff *aofBuffer) flush() {

	writingStatus := atomic.LoadInt32(&buff.writing)

	// 当前有正在发生的写入操作
	if writingStatus >= 0 {
		return
	}

	// 通知协程进行写入操作
	buff.notification <- struct{}{}

}

func (buff *aofBuffer) append(bytes []byte) {

	if buff.writer == nil {
		return
	}

	ok := buff.pages[buff.appendSeq%buff.pageSize].append(bytes)
	if !ok {
		buff.appendSeq++

		// 如果自加后追赶上刷盘
		if buff.appendSeq-buff.flushSeq == buff.pageSize {
			buff.flushBuffer()
		}

		buff.pages[buff.appendSeq%buff.pageSize].append(bytes)
	}
}
