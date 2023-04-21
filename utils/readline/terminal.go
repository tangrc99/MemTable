package readline

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"syscall"
)

type Termios syscall.Termios

type Line struct {
	insertPos int
	content   []byte
}

func newLine() *Line {
	return &Line{
		insertPos: 0,
		content:   []byte{},
	}
}

func newLineFrom(content []byte) *Line {
	return &Line{
		insertPos: len(content),
		content:   content,
	}
}

// write 将字节写入当前行中，返回当前插入后 offset 以及需要刷新的缓冲区内容
func (l *Line) write(c byte) (int, []byte) {
	if l.insertPos == len(l.content) {
		l.insertPos++
		l.content = append(l.content, c)
		return l.insertPos, l.content[l.insertPos-1:]
	}
	l.content = append(l.content, byte(0))
	copy(l.content[l.insertPos+1:], l.content[l.insertPos:])
	l.content[l.insertPos] = c
	l.insertPos++
	return l.insertPos, l.content[l.insertPos-1:]
}

// delete 删除当前位置下的字符，返回删除后的下标以及要刷新的缓冲区内容
func (l *Line) delete() (int, []byte) {
	if l.insertPos == 0 {
		return 0, []byte{}
	}
	l.content = append(l.content[:l.insertPos-1], l.content[l.insertPos:]...)
	l.insertPos--
	return l.insertPos, append(l.content[l.insertPos:], ' ')
}

// MoveCursor 会根据 offset 移动插入位置
func (l *Line) moveCursor(offset int) {
	l.insertPos += offset
	if l.insertPos < 0 {
		l.insertPos = 0
	} else if l.insertPos > len(l.content) {
		l.insertPos = len(l.content)
	}
}

// head 返回当前位置之前具有几个字节
func (l *Line) head() int {
	return l.insertPos
}

// tail 返回当前位置之后具有几个字节
func (l *Line) tail() int {
	return len(l.content) - l.insertPos
}

// firstWord 返回当前行的第一个单词
func (l *Line) firstWord() []byte {
	for i := 0; i < len(l.content); i++ {
		if l.content[i] == ' ' {
			return l.content[:i]
		}
	}
	return l.content[:]
}

// currentWord 返回当前修改的单词
func (l *Line) currentWord() []byte {

	// 找到当前单词的起点
	i, j := l.insertPos-1, l.insertPos
	for ; i >= 0; i-- {
		if l.content[i] == ' ' {
			break
		}
	}
	for ; j < len(l.content); j++ {
		if l.content[j] == ' ' {
			break
		}
	}
	return l.content[i+1 : j]
}

type TerminalCommand func(input [][]byte, abort bool) int

// Terminal 是对当前终端显示内容的一个抽象，负责维护终端上的光标以及内容
type Terminal struct {
	content  []*Line // 输入缓存
	line     int     // 当前操作的行
	buffer   []byte  // 用于处理多字节的命令
	finished bool    // 是否解析完毕
	aborted  bool    // 因为信号而退出

	histories [][]byte // 执行成功过的历史命令
	hlimit    int      //  历史命令上线
	hpos      int      // 当前显示的历史命令
	hauto     bool     // 是否自动存储历史命令

	completer    *Completer // 补全器
	highlight    int        // 补全信息高亮显示的位置
	targets      []string   // 当前正在显示的补全信息
	helper       string     // 当前正在显示的帮助信息
	displayLimit int        // 一次最大显示的补全个数
	displayedLen int

	prefix string // 输入行的前缀提示符
}

func NewTerminal() *Terminal {
	return &Terminal{
		content:      []*Line{newLine()},
		line:         0,
		buffer:       make([]byte, 0),
		completer:    NewCompleter(),
		displayLimit: 8,
		highlight:    -1,
		hlimit:       20,
		hauto:        true,
		prefix:       "> ",
	}
}

// ReadLine 阻塞并解析一行命令，如果期间发生信号中断，abort 标识位为 true
func (t *Terminal) ReadLine() (cmd [][]byte, abort bool) {

	old := DisableTerminal()

	FlushString(t.prefix)

	input := make([]byte, 1)

	for !t.finished {
		if _, err := os.Stdin.Read(input); err == io.EOF {
			break
		}
		t.handleInput(input[0])
	}

	// 收集每一行字符串
	var c []byte
	for _, line := range t.content {
		c = append(c, line.content...)
	}

	// 记录历史命令
	if t.hauto {
		t.histories = append(t.histories, c)
		if len(t.histories) > t.hlimit {
			t.histories = t.histories[1:]
		}
	}

	t.clear()
	// 恢复终端设置
	_ = setTermios(int(os.Stdout.Fd()), old)

	return SplitRepeatableSeg(c, ' '), t.aborted
}

// ReadLineAndExec 读取一行命令并且执行；如果执行返回值为 0，记录该命令。
func (t *Terminal) ReadLineAndExec(f TerminalCommand) {

	old := DisableTerminal()
	FlushString(t.prefix)

	input := make([]byte, 1)

	for !t.finished {
		if _, err := os.Stdin.Read(input); err == io.EOF {
			break
		}
		t.handleInput(input[0])
	}

	// 收集每一行字符串
	var c []byte
	for _, line := range t.content {
		c = append(c, line.content...)
	}

	command := SplitRepeatableSeg(c, ' ')

	// 如果运行成功，记录历史命令
	if f(command, t.aborted) == 0 {
		t.histories = append(t.histories, c)
		if len(t.histories) > t.hlimit {
			t.histories = t.histories[1:]
		}
	}

	t.clear()
	// 恢复终端设置
	_ = setTermios(int(os.Stdout.Fd()), old)
}

func (t *Terminal) StoreHistory(line []byte) {
	t.histories = append(t.histories, line)
	if len(t.histories) > t.hlimit {
		t.histories = t.histories[1:]
	}
}

func (t *Terminal) WithCompleter(completer *Completer) *Terminal {
	t.completer = completer
	return t
}

func (t *Terminal) WithHistoryLimitation(max int) *Terminal {
	t.hlimit = max
	return t
}

// WithAutoRecordHistory 是否允许自动记录命令。使用 ReadLine 接口时，函数是由外界执行的，可能会记录一些执行失败的命令；
// 可以通过该函数关闭自动记录，并且手动记录成功的命令。
func (t *Terminal) WithAutoRecordHistory(enable bool) *Terminal {
	t.hauto = enable
	return t
}

func (t *Terminal) WithPrefix(prefix string) *Terminal {
	t.prefix = prefix
	return t
}

func (t *Terminal) WithDisplayLimit(limit int) *Terminal {
	if limit > 0 {
		t.displayLimit = limit
	}
	return t
}

/* ---------------------------------------------------------------------------
* Internal Implementation
* ------------------------------------------------------------------------- */

func (t *Terminal) currentLine() *Line {
	return t.content[t.line]
}

// MoveCursor 模拟移动光标，x 与 y 是偏移量而非绝对位置
func (t *Terminal) moveCursor(x, y int) {

	// 防止超出范围
	if x > t.currentLine().tail() {
		x = t.currentLine().tail()
	} else if x+t.currentLine().head() < 0 {
		x = -t.currentLine().head()
	}

	if y+t.line < 0 {
		y = -t.line
		t.line = 0
	} else if y+t.line+1 > len(t.content) {
		y = len(t.content) - t.line - 1
		t.line = len(t.content) - 1
	}

	if x != 0 {
		t.currentLine().moveCursor(x)
	}

	MoveCursor(x, y)
}

// insert 写入数据到终端
func (t *Terminal) insert(input byte) {
	_, content := t.currentLine().write(input)
	Flush(content)
	MoveCursor(-len(content)+1, 0)
}

func (t *Terminal) delete() {
	if t.currentLine().head() == 0 {
		return
	}
	_, content := t.currentLine().delete()
	//os.Stdout.WriteString("\b \b")
	MoveCursor(-1, 0)
	Flush(content)
	MoveCursor(-len(content), 0)
}

// lastByte 返回当前行的最后一个字符，如果行为空，返回 0
func (t *Terminal) lastByte() byte {
	c := t.currentLine().content
	if len(c) == 0 {
		return 0
	}
	return c[len(c)-1]
}

// newLine 创建一个新行，"\\n"会导致换行出现
func (t *Terminal) newLine() {

	t.currentLine().delete()
	t.content = append(t.content, newLine())
	MoveCursor(-t.currentLine().head()-1, 1)
	t.line++
}

func (t *Terminal) maybeDisplayHelper() {

	if t.completer == nil {
		return
	}

	w := string(t.content[0].firstWord())
	if w == "" {
		return
	}
	t.helper, _ = t.completer.GetHelper(w)
	if t.helper == "" {
		return
	}

	// Display
	x, y := ReadCursor()
	//MoveCursorTo(0, y+1)
	FlushString(fmt.Sprintf("\n\033[;37m%s\033[0m ", t.helper))

	// 判断终端是否写满
	_, cy := ReadCursor()
	if cy == y {
		MoveCursorTo(x, y-1)
	} else {
		MoveCursorTo(x, y)
	}
}

func (t *Terminal) maybeClearHelper() {

	if len(t.helper) == 0 {
		return
	}

	x, y := ReadCursor()
	MoveCursorTo(0, y+1)

	Flush(bytes.Repeat([]byte{' '}, len(t.helper)))

	MoveCursorTo(x, y)
	t.helper = ""
}

func (t *Terminal) handleInput(input byte) {

	// 处理控制类型输入
	if len(t.buffer) != 0 {
		keyHandlerMap[ESC](t, input)
		return
	}

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

}

// clear 清除当前行的缓存信息
func (t *Terminal) clear() {
	t.buffer = []byte{}
	t.content = []*Line{newLine()}
	t.line = 0
	t.helper = ""
	t.targets = []string{}
	t.finished = false
	t.hpos = len(t.histories)
}

// finish 表示完成当前行的读取
func (t *Terminal) finish() {
	t.finished = true
	FlushString("\n")
}

func (t *Terminal) abort() {
	t.aborted = true
	t.finish()
}

func (t *Terminal) maybeClearCompletion() {
	if t.highlight >= 0 {
		t.clearCompletion()
	}
}

// completionDisplayed 判断当前是否显示了补全内容
func (t *Terminal) completionDisplayed() bool {
	return t.highlight >= 0
}

// clearCompletion 清除已经显示的补全命令
func (t *Terminal) clearCompletion() {

	x, y := ReadCursor()
	MoveCursorTo(0, y+1)

	Flush(bytes.Repeat([]byte{' '}, t.displayedLen))

	MoveCursorTo(x, y)
	t.targets = []string{}
	t.highlight = -1
	t.displayedLen = 0
}

// selectCompletion 切换选择的补全命令
func (t *Terminal) selectCompletion(x, y int) {

	// 上下翻页不直接循环
	t.highlight += y * t.displayLimit
	if t.highlight < 0 {
		t.highlight = 0
	} else if t.highlight >= len(t.targets) {
		t.highlight = len(t.targets) - 1
	}

	t.highlight = (t.highlight + x) % len(t.targets)
	if t.highlight < 0 {
		t.highlight = len(t.targets) - 1
	}

	ox, oy := ReadCursor()

	// 清理之前的输出
	MoveCursorTo(0, oy+1)
	Flush(bytes.Repeat([]byte{' '}, t.displayedLen))

	MoveCursorTo(0, oy+1)

	toDisplay := t.targets
	toHighlight := t.highlight
	// 防止一次显示过多选项
	if len(t.targets) > t.displayLimit {
		start := t.highlight / t.displayLimit
		toDisplay = t.targets[start*t.displayLimit : (start+1)*t.displayLimit]
		toHighlight = t.highlight - start*t.displayLimit
	}

	t.displayedLen = 0
	for i := range toDisplay {
		if i == toHighlight {
			FlushString(fmt.Sprintf("\033[47;37m%s\033[0m ", toDisplay[i]))
		} else {
			FlushString(toDisplay[i] + " ")
		}
		t.displayedLen += len(toDisplay[i]) + 1
	}

	MoveCursorTo(ox, oy)
}

// doComplete 补全选中的命令
func (t *Terminal) doComplete() {
	word := t.currentLine().currentWord()
	if len(word) == 0 {
		return
	}
	target := t.targets[t.highlight]

	for _, b := range target[len(word):] {
		t.insert(byte(b))
	}

	t.clearCompletion()
}

// showCompletions 显示可能的命令
func (t *Terminal) showCompletions() bool {

	if t.completer == nil || len(t.currentLine().content) == 0 {
		return false
	}

	word := t.currentLine().currentWord()
	if len(word) == 0 {
		return false
	}

	// 如果没有正在显示，则读取
	if !t.completionDisplayed() {
		t.targets = t.completer.Query(string(word))
	}

	// 没有可以匹配的选项
	if len(t.targets) == 0 {
		return true
	} else if len(t.targets) == 1 {
		// 单一匹配，直接补全，并且显示提示
		for _, b := range t.targets[0][len(word):] {
			t.insert(byte(b))
		}
		// 如果完成单词补全，显示帮助选项
		t.maybeDisplayHelper()
		return true
	}

	t.highlight = (t.highlight + 1) % len(t.targets)

	x, y := ReadCursor()

	t.maybeClearHelper()

	// 切换到下一行，如果写满则换行
	FlushString("\n")
	// 清理之前的输出
	Flush(bytes.Repeat([]byte{' '}, t.displayedLen))
	MoveCursor(-t.displayedLen, 0)

	toDisplay := t.targets
	toHighlight := t.highlight
	// 防止一次显示过多选项
	if len(t.targets) > t.displayLimit {
		start := t.highlight / t.displayLimit
		toDisplay = t.targets[start*t.displayLimit : (start+1)*t.displayLimit]
		toHighlight = t.highlight - start*t.displayLimit
	}

	t.displayedLen = 0
	for i := range toDisplay {
		if i == toHighlight {
			FlushString(fmt.Sprintf("\033[47;37m%s\033[0m ", toDisplay[i]))
		} else {
			FlushString(toDisplay[i] + " ")
		}
		t.displayedLen += len(toDisplay[i]) + 1
	}

	// 判断终端是否写满
	_, cy := ReadCursor()
	if cy == y {
		MoveCursorTo(x, y-1)
	} else {
		MoveCursorTo(x, y)
	}
	return true
}

func (t *Terminal) switchHistory(offset int) {

	// nothing to do
	if len(t.histories) == 0 {
		return
	}

	t.hpos += offset

	if t.hpos >= len(t.histories) {
		t.hpos = len(t.histories)
	} else if t.hpos <= 0 {
		t.hpos = 0
	}

	var toDisplay []byte
	if t.hpos < len(t.histories) {
		toDisplay = t.histories[t.hpos]
	}

	// 清除现有的行，不直接清行，防止自动换行导致无法全部清除
	head := t.currentLine().head()

	t.currentLine().moveCursor(-head)
	MoveCursor(-head, 0)

	x, y := ReadCursor()
	Flush(bytes.Repeat([]byte{' '}, len(t.currentLine().content)))
	MoveCursorTo(x, y)

	t.content[t.line] = newLineFrom(toDisplay)
	Flush(toDisplay)
}
