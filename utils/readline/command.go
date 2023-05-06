package readline

import (
	"bytes"
	"fmt"
)

// InternalCommand 是可以被注册在 Terminal 中的命令。如果输入匹配命令，则会直接执行命令，而不会返回 line。
// args[0] 是 command name, args[1:] 是输入参数
type InternalCommand func(t *Terminal, args [][]byte)

var commandTable = map[string]InternalCommand{}

func commandHelp(_ *Terminal, _ [][]byte) {
	format := "  %-20s: %s\n"

	fmt.Printf("This is default helper.\n\n")

	fmt.Printf(format, "[control]+[R]", "enter search mode or search.")
	fmt.Printf(format, "[TAB]", "enter completion mode.")
	fmt.Printf(format, "[LEFT]/[RIGHT]", "select completion.")
	fmt.Printf(format, "[UP]/[DOWN]", "select history command.")
	fmt.Printf(format, "[ESC]+[ESC]", "quit search or completion mode.")

	fmt.Printf(format, "\"help\"", "show this helper.")
	fmt.Printf(format, "\"quit\"", "quit.")
	fmt.Printf(format, "\"history\"", "show histories.")
}

func commandQuit(t *Terminal, _ [][]byte) {
	t.aborted = true
}

func commandHistory(t *Terminal, command [][]byte) {
	if len(command) == 2 && bytes.Equal(command[1], []byte("clean")) {
		t.histories.clean()
		return
	}

	h := t.histories.histories()
	for i := range h {
		fmt.Printf("%d) %s\n", i, h[i])
	}
}

func addDefaultCommands(c *Completer) {
	if c == nil {
		return
	}
	if !c.Exist("help") {
		c.Register(NewHint("help", "_"))
		commandTable["help"] = commandHelp
	}
	if !c.Exist("quit") {
		c.Register(NewHint("quit", "_"))
		commandTable["quit"] = commandQuit
	}
	if !c.Exist("history") {
		c.Register(NewHint("history", "_"))
		commandTable["history"] = commandHistory
	}
}

func RegisterInternalCommand(c *Completer, name, helper string, cmd InternalCommand) {
	commandTable[name] = cmd
	c.Register(NewHint(name, helper))
}
