package server

import (
	"os"
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

// sync 返回失败代表当前 page 有待写入内容
func (buff *bufferPage) sync(writer *os.File) bool {

	if writer == nil {
		return true
	}

	wn := 0

	for wn != buff.pos {

		w, err := writer.Write(buff.content[wn:buff.pos])
		if err != nil {
			println(err.Error())
			os.Exit(-1)
			//logger.Error("Aof buffer: ", err)
		}
		wn += w
	}

	buff.pos = 0

	return true
}

type AOFBuffer struct {
	writer *os.File

	flush    int64 // 当前刷盘序列号
	appends  int64 // 当前写入序列号
	pages    []*bufferPage
	pageSize int64
}

func NewAOFBuffer(filename string) *AOFBuffer {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		println(err.Error())
	}

	buffers := AOFBuffer{
		writer:   file,
		pages:    make([]*bufferPage, 3),
		flush:    0,
		appends:  0,
		pageSize: 3,
	}
	buffers.pages[0] = newBufferPage(65536)
	buffers.pages[1] = newBufferPage(65536)
	buffers.pages[2] = newBufferPage(65536)

	return &buffers
}

func (buff *AOFBuffer) Append(bytes []byte) {

	if buff.writer == nil {
		return
	}

	ok := buff.pages[buff.appends%buff.pageSize].append(bytes)
	if !ok {
		buff.appends++

		// 如果自加后追赶上刷盘
		if buff.appends-buff.flush == buff.pageSize {
			buff.Flush()
		}

		buff.pages[buff.appends%buff.pageSize].append(bytes)
	}
}

func (buff *AOFBuffer) Flush() {

	if buff.writer == nil {
		return
	}

	buff.pages[buff.flush%buff.pageSize].sync(buff.writer)
	if buff.flush == buff.appends {
		return
	}
	buff.flush++
}

func (buff *AOFBuffer) Sync() {

	err := buff.writer.Sync()
	if err != nil {
		return
	}
}
