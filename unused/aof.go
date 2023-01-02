package unused

import (
	"MemTable/logger"
	"os"
	"sync"
	"sync/atomic"
)

type spinLock struct {
	flag int32
}

func (lock *spinLock) Lock() {
	for !atomic.CompareAndSwapInt32(&lock.flag, 0, 1) {
	}
}

func (lock *spinLock) Unlock() {
	atomic.StoreInt32(&lock.flag, 0)
}

// bufferPage 是一个单写缓冲区，如果需要多写需要加锁
type bufferPage struct {
	content []byte
	pos     int
	max     int
	mtx     sync.Mutex

	wr int // 剩余的写入者数量
}

func newBufferPage(maxSize int) *bufferPage {
	return &bufferPage{
		content: make([]byte, maxSize),
		pos:     0,
		max:     maxSize,
		wr:      0,
	}
}

// append 返回失败代表当前 page 已经写满
func (buff *bufferPage) append(bytes []byte) bool {
	buff.mtx.Lock()
	buff.wr++

	defer func() {
		buff.wr--
		buff.mtx.Unlock()
	}()
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

	buff.mtx.Lock()
	defer buff.mtx.Unlock()

	if writer == nil {
		return true
	}

	if buff.wr > 0 {
		return false
	}

	wn := 0

	for wn != buff.pos {

		w, err := writer.Write(buff.content[wn:buff.pos])
		if err != nil {
			logger.Error("Aof buffer: ", err)
		}
		wn += w
	}

	buff.pos = 0

	return true
}

type AOFBuffer struct {
	writer *os.File
	lock   spinLock

	flush    int32 // 当前刷盘序列号
	appends  int32 // 当前写入序列号
	pages    []*bufferPage
	pageSize int32
}

func NewAOFBuffer(filename string) *AOFBuffer {
	file, _ := os.OpenFile(filename, os.O_APPEND|os.O_CREATE, 0666)

	buffers := AOFBuffer{
		writer: file,
		lock:   spinLock{0},
		pages:  make([]*bufferPage, 3),
	}
	buffers.pages[0] = newBufferPage(65536)
	buffers.pages[1] = newBufferPage(65536)
	buffers.pages[2] = newBufferPage(65536)

	return &buffers
}

//func (buffers *AOFBuffer) Pos() (int, int) {
//	buffers.lock.Lock()
//	appends := buffers.appends
//	flush := buffers.flush
//	buffers.lock.Unlock()
//	return appends, flush
//}

func (buffers *AOFBuffer) Append(buffer []byte) {

	if buffers.writer == nil {
		return
	}

	flush := atomic.LoadInt32(&buffers.flush)
	appends := atomic.LoadInt32(&buffers.appends)

	// 写入缓冲区
	ok := buffers.pages[appends].append(buffer)

	for !ok {

		flush = atomic.LoadInt32(&buffers.flush)
		appends = atomic.LoadInt32(&buffers.appends)

		if flush == appends {
			nAppends := (appends + 1) % buffers.pageSize
			atomic.CompareAndSwapInt32(&buffers.appends, appends, nAppends)
			// 如果失败，代表缓冲区已经移动，不用管
			appends = atomic.LoadInt32(&buffers.appends)
		}

		ok = buffers.pages[appends].append(buffer)

	}

}

func (buffers *AOFBuffer) Sync() {

	if buffers.writer == nil || buffers.flush > buffers.appends {
		return
	}

	flush := atomic.LoadInt32(&buffers.flush)
	appends := atomic.LoadInt32(&buffers.appends)

	if flush == appends {
		nAppends := (appends + 1) % buffers.pageSize
		atomic.CompareAndSwapInt32(&buffers.appends, appends, nAppends)
		// 如果失败，代表缓冲区已经移动，不用管
	}

	// 在当前页面完成写入后写缓冲区
	for buffers.pages[flush].sync(buffers.writer) == false {

	}
}
