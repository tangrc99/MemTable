package cmd

import (
	"MemTable/db"
	"MemTable/resp"
	"strings"
)

type ExecStatus int

const (
	RD ExecStatus = iota
	WR
)

type Command = func(base *db.DataBase, cmd [][]byte) resp.RedisData

var CommandTable = make(map[string]Command)
var WriteCommands = make(map[string]struct{})

func RegisterCommand(name string, cmd Command, status ExecStatus) {

	CommandTable[name] = cmd

	if status == WR {
		WriteCommands[name] = struct{}{}
	}
}

func init() {
	RegisterKeyCommands()
	RegisterStringCommands()
	RegisterSetCommands()
	RegisterListCommands()
	RegisterHashCommands()
	RegisterZSetCommands()

	RegisterBitMapCommands()
}

func ExecCommand(base *db.DataBase, cmd [][]byte) (resp.RedisData, bool) {

	if len(cmd) == 0 {
		return resp.MakeErrorData("error: empty command"), false
	}

	_, isWriteCommand := WriteCommands[strings.ToLower(string(cmd[0]))]

	f, ok := CommandTable[strings.ToLower(string(cmd[0]))]
	if !ok {
		return resp.MakeErrorData("error: unsupported command"), isWriteCommand
	}

	return f(base, cmd), isWriteCommand
}
