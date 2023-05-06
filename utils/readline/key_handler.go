package readline

import (
	"bytes"
	"fmt"
	"syscall"
)

const (
	SIGINT    byte = 3
	TAB       byte = 9
	CLEAR     byte = 12
	ENTER     byte = 13
	SEARCH    byte = 18
	SIGTSTP   byte = 26
	ESC       byte = 27
	SIGQUIT   byte = 28
	BACKSPACE byte = 127
)

var keyHandlerMap = map[byte]keyHandler{}

type keyHandler func(terminal *Terminal, input byte)

func keyHandlerESC(t *Terminal, input byte) {

	// 判断用户是否输入了特殊控制字符
	if len(t.buffer) == 1 && input != '[' && input != ESC {
		handler, exist := keyHandlerMap[input]
		if exist {
			handler(t, input)
			return
		}

		if IsOrdinaryInput(input) {
			keyHandlerAlpha(t, input)
		} else {
			panic(fmt.Sprintf("Read Unknown char '%d'", input))
		}
		return
	}

	t.buffer = append(t.buffer, input)

	if len(t.buffer) > 5 {
		t.buffer = []byte{}
		return
	}

	for i := 1; i < len(t.buffer); i++ {
		if t.buffer[i] == ESC {
			t.buffer = t.buffer[i:]
			i = 1
			t.maybeClearCompletion()
			t.maybeClearSearch()
		}
	}

	if bytes.Equal(t.buffer, []byte{27, '[', 'D'}) {
		if t.inSearchMode() {
			t.quitSearchMode()
			return
		}

		if t.highlight >= 0 {
			t.selectCompletion(-1, 0)
			t.buffer = []byte{}
			return
		}
		t.moveCursor(-1, 0)
		t.buffer = []byte{}

	} else if bytes.Equal(t.buffer, []byte{27, '[', 'C'}) {
		if t.inSearchMode() {
			t.quitSearchMode()
			return
		}
		if t.highlight >= 0 {
			t.selectCompletion(1, 0)
			t.buffer = []byte{}
			return
		}
		t.moveCursor(1, 0)
		t.buffer = []byte{}
	} else if bytes.Equal(t.buffer, []byte{27, '[', 'A'}) {
		if t.highlight >= 0 {
			t.selectCompletion(0, -1)
			t.buffer = []byte{}
			return
		}
		t.switchHistory(-1)
		t.buffer = []byte{}
	} else if bytes.Equal(t.buffer, []byte{27, '[', 'B'}) {
		if t.highlight >= 0 {
			t.selectCompletion(0, 1)
			t.buffer = []byte{}
			return
		}
		t.switchHistory(1)
		t.buffer = []byte{}
	}

}

func keyHandlerEnter(t *Terminal, _ byte) {

	if t.inSearchMode() {
		t.quitSearchMode()
		return
	}

	if t.highlight >= 0 {
		t.doComplete()
		t.maybeDisplayHelper()
		return
	}
	t.maybeClearHelper()
	t.maybeClearSearch()

	if t.lastByte() == '\\' {
		t.newLine()
		return
	}

	t.finish()

}

func keyHandlerBackspace(t *Terminal, _ byte) {

	if t.inSearchMode() {
		if len(t.search) == 0 {
			return
		}
		t.search = t.search[:len(t.search)-1]
		t.displaySearch()
		return
	}

	t.maybeClearCompletion()
	t.maybeClearHelper()

	t.delete()
	t.maybeDisplayHelper()

}

// keyHandlerSIGINT 处理信号 control-C
func keyHandlerSIGINT(t *Terminal, _ byte) {
	t.maybeClearCompletion()
	t.maybeClearHelper()
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	t.abort()
}

// keyHandlerSIGTSTP 处理信号 control-Z
func keyHandlerSIGTSTP(t *Terminal, _ byte) {
	t.maybeClearCompletion()
	t.maybeClearHelper()
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGTSTP)
	t.finished = true
	t.abort()
}

// keyHandlerSIGQUIT 处理信号 control-\
func keyHandlerSIGQUIT(t *Terminal, _ byte) {
	t.maybeClearCompletion()
	t.maybeClearHelper()
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGQUIT)
	t.abort()
}

func keyHandlerTab(t *Terminal, _ byte) {
	if t.inSearchMode() {
		t.search = append(t.search, []byte("    ")...)
		t.displaySearch()
		return
	}
	if !t.showCompletions() {
		t.insert(' ')
		t.insert(' ')
		t.insert(' ')
		t.insert(' ')
		t.maybeClearHelper()
	}
}

func keyHandlerAlpha(t *Terminal, input byte) {

	if t.inSearchMode() {
		t.search = append(t.search, input)
		t.displaySearch()
		return
	}

	t.maybeClearHelper()
	t.maybeClearCompletion()
	t.insert(input)
	t.maybeDisplayHelper()
}

func keyHandlerSearch(t *Terminal, _ byte) {
	if !t.inSearchMode() {
		t.search = t.bytes()
		t.clearCurrentLine()
		t.displaySearch()
		return
	}
	t.searchHistory()
}

func init() {
	keyHandlerMap[ESC] = keyHandlerESC
	keyHandlerMap[TAB] = keyHandlerTab
	keyHandlerMap[ENTER] = keyHandlerEnter
	keyHandlerMap[BACKSPACE] = keyHandlerBackspace
	keyHandlerMap[SIGQUIT] = keyHandlerSIGQUIT
	keyHandlerMap[SIGTSTP] = keyHandlerSIGTSTP
	keyHandlerMap[SIGINT] = keyHandlerSIGINT
	keyHandlerMap[SEARCH] = keyHandlerSearch
	//keyHandlerMap[] = keyHandler

}
