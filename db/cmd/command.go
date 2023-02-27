package cmd

import (
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/resp"
	"strings"
)

// ExecStatus 标识一个 command 是否为写操作
type ExecStatus int

const (
	// RD 标识 command 为只读操作
	RD ExecStatus = iota
	// WR 标识 command 为写操作
	WR
)

type command = func(base *db.DataBase, cmd [][]byte) resp.RedisData

var commandTable = make(map[string]command)
var writeCommands = make(map[string]struct{})

func registerCommand(name string, cmd command, status ExecStatus) {

	commandTable[name] = cmd

	if status == WR {
		writeCommands[name] = struct{}{}
	}
}

func init() {
	registerKeyCommands()
	registerStringCommands()
	registerSetCommands()
	registerListCommands()
	registerHashCommands()
	registerZSetCommands()
	registerBitMapCommands()
	RegisterBloomFilterCommands()
}

// ExecCommand 在指定的数据库中执行命令, enableWrite 参数用于控制是否允许写操作执行，函数将返回 command 执行结果以及 command 类型
func ExecCommand(base *db.DataBase, cmd [][]byte, enableWrite bool) (res resp.RedisData, isWriteCommand bool) {

	if len(cmd) == 0 {
		return resp.MakeErrorData("error: empty command"), false
	}

	_, isWriteCommand = writeCommands[strings.ToLower(string(cmd[0]))]

	if isWriteCommand && !enableWrite {
		return resp.MakeErrorData("ERR READONLY You can't write against a read only slave"), false
	}

	f, ok := commandTable[strings.ToLower(string(cmd[0]))]
	if !ok {
		return resp.MakeErrorData("error: unsupported command"), isWriteCommand
	}

	return f(base, cmd), isWriteCommand
}

func IsCommandExist(cmd string) bool {
	_, exist := commandTable[strings.ToLower(string(cmd[0]))]
	return exist
}

func IsWriteCommand(cmd string) bool {
	_, ok := writeCommands[cmd]
	return ok
}

func IsRandCommand(cmd string) bool {
	return cmd == "randomkey" || cmd == "srandmember"
}
