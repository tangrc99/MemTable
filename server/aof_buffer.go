package server

import (
	"github.com/tangrc99/MemTable/logger"
	"os"
	"sync/atomic"
)

const bufferPageSize = 3
const maxBufferPageCapacity = 65536

// appendResult 是 bufferPage.append 函数的返回值类型
type appendResult int

const (
	// appendSuccess 代表写入成功，不需要额外动作
	appendSuccess appendResult = iota
	// appendFail 代表没有写入当前页，需要切换并且再次写入
	appendFail
	// appendAppendix 代表 AOF 写入了当前页的尾部，需要切换页，并且不需要再次写入
	appendAppendix
)

// bufferPage 是一个单写缓冲区，如果需要多写需要加锁
type bufferPage struct {
	content  []byte   // page 内容
	pos      int      // 当前写入位置
	max      int      // page 最大值
	appendix [][]byte // 用于存储大 AOF
}

func newBufferPage(maxSize int) *bufferPage {
	return &bufferPage{
		content:  make([]byte, maxSize),
		pos:      0,
		max:      maxSize,
		appendix: make([][]byte, 0),
	}
}

// append 返回失败代表当前 page 已经写满
func (buff *bufferPage) append(bytes []byte) appendResult {

	if len(buff.appendix) > 0 {
		buff.appendix = append(buff.appendix, bytes)
		return appendAppendix
	}

	l := len(bytes)

	if l+buff.pos > buff.max {
		// 如果长度超过了 page 的最大值，应该单独开辟缓冲区写入
		if l > buff.max {
			buff.appendix = append(buff.appendix, bytes)
			return appendAppendix
		}
		return appendFail
	}

	copy(buff.content[buff.pos:], bytes)
	buff.pos += l
	return appendSuccess
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
			logger.Panicf("Aof: %s", err.Error())
		}
		wn += w
	}

	// 刷盘 appendix 中的内容
	for i := range buff.appendix {
		_, err := writer.Write(buff.appendix[i])
		if err != nil {
			logger.Panicf("Aof: %s", err.Error())
		}
	}

	buff.pos = 0
	buff.appendix = [][]byte{}
	return true
}

// aofBuffer 是维护了 AOF 缓冲区，它内部是一个 link_buffer 结构的缓冲区。缓冲区的硬盘写入操作是异步的，所有的操作保证成功，或抛出异常。
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
		pages:        make([]*bufferPage, bufferPageSize),
		flushSeq:     0,
		appendSeq:    0,
		pageSize:     3,
		writing:      0,
		notification: make(chan struct{}),
		quitFlag:     make(chan struct{}),
	}

	for i := range buffers.pages {
		buffers.pages[i] = newBufferPage(maxBufferPageCapacity)
	}

	go func() {
		buffers.asyncTask()
		logger.Info("AOF: AOF Goroutine exits")
	}()

	return buffers
}

func (buff *aofBuffer) asyncTask() {
	q := false
	for !q {
		select {
		case <-buff.notification:
			// 写入 os 缓冲区
			atomic.StoreInt32(&buff.writing, 1)
			buff.flushBuffer()

			// os 缓冲区写入硬盘
			atomic.StoreInt32(&buff.writing, 2)
			buff.syncToDisk()

			atomic.StoreInt32(&buff.writing, 0)

		case <-buff.quitFlag:
			q = true
		}
	}
}

// flushBuffer 会将当前页写入到硬盘中
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
	if writingStatus > 0 {
		return
	}

	// 通知协程进行写入操作
	buff.notification <- struct{}{}
}

// append 将内容写入到 AOF 缓冲区中，如果当前缓冲区已满，函数会阻塞直到刷盘清理出一部分可写入的缓冲区
func (buff *aofBuffer) append(bytes []byte) {

	if buff.writer == nil {
		return
	}

	result := buff.pages[buff.appendSeq%buff.pageSize].append(bytes)

	if result != appendSuccess {
		buff.appendSeq++

		// 如果自加后追赶上刷盘
		if buff.appendSeq-buff.flushSeq == buff.pageSize {
			buff.flushBuffer()
		}

		// 如果在上一页中插入失败，需要再一次尝试写入当前页
		if result == appendFail {
			buff.pages[buff.appendSeq%buff.pageSize].append(bytes)
		}
	}
}
