package readline

import (
	"bytes"
	"syscall"
)

const (
	SIGINT    byte = 3
	TAB       byte = 9
	ENTER     byte = 13
	SIGTSTP   byte = 26
	ESC       byte = 27
	SIGQUIT   byte = 28
	BACKSPACE byte = 127
)

var keyHandlerMap = map[byte]keyHandler{}

type keyHandler func(terminal *Terminal, input byte)

func keyHandlerESC(t *Terminal, input byte) {

	t.buffer = append(t.buffer, input)

	for i := 1; i < len(t.buffer); i++ {
		if t.buffer[i] == ESC {
			t.buffer = t.buffer[i:]
			i = 1
			if t.completionDisplayed() {
				t.clearCompletion()
			}
		}
	}

	if bytes.Equal(t.buffer, []byte{27, '[', 'D'}) {
		if t.highlight >= 0 {
			t.selectCompletion(-1, 0)
			t.buffer = []byte{}
			return
		}
		t.moveCursor(-1, 0)
		t.buffer = []byte{}
	} else if bytes.Equal(t.buffer, []byte{27, '[', 'C'}) {
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
	if t.highlight >= 0 {
		t.doComplete()
		t.maybeDisplayHelper()
		return
	}
	t.maybeClearHelper()

	if t.lastByte() == '\\' {
		t.newLine()
		return
	}

	FlushString("\n")
	t.finish()

}

func keyHandlerBackspace(t *Terminal, _ byte) {
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
}

// keyHandlerSIGTSTP 处理信号 control-Z
func keyHandlerSIGTSTP(t *Terminal, _ byte) {
	t.maybeClearCompletion()
	t.maybeClearHelper()
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGTSTP)
}

// keyHandlerSIGQUIT 处理信号 control-\
func keyHandlerSIGQUIT(t *Terminal, _ byte) {
	t.maybeClearCompletion()
	t.maybeClearHelper()
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGQUIT)
}

func keyHandlerTab(t *Terminal, _ byte) {
	if !t.showCompletions() {
		t.insert(' ')
		t.insert(' ')
		t.insert(' ')
		t.insert(' ')
		t.maybeClearHelper()
	}
}

func keyHandlerAlpha(t *Terminal, input byte) {

	t.maybeClearHelper()
	t.maybeClearCompletion()
	t.insert(input)
	t.maybeDisplayHelper()
}

func init() {
	keyHandlerMap[ESC] = keyHandlerESC
	keyHandlerMap[TAB] = keyHandlerTab
	keyHandlerMap[ENTER] = keyHandlerEnter
	keyHandlerMap[BACKSPACE] = keyHandlerBackspace
	keyHandlerMap[SIGQUIT] = keyHandlerSIGQUIT
	keyHandlerMap[SIGTSTP] = keyHandlerSIGTSTP
	keyHandlerMap[SIGINT] = keyHandlerSIGINT
	//keyHandlerMap[] = keyHandler

}
