package server

import (
	"MemTable/resp"
	"fmt"
	"strings"
)

type ExecStatus int

const (
	RD ExecStatus = iota
	WR
)

type Command = func(server *Server, cli *Client, cmd [][]byte) resp.RedisData

var CommandTable = make(map[string]Command)
var WriteCommands = make(map[string]struct{})

func RegisterCommand(name string, cmd Command, status ExecStatus) {
	CommandTable[name] = cmd
	if status == WR {
		WriteCommands[name] = struct{}{}
	}
}

func init() {
	RegisterPubSubCommands()
	RegisterConnectionCommands()
	RegisterServerCommand()
	RegisterTransactionCommand()
}

func ExecCommand(server *Server, cli *Client, cmd [][]byte) (resp.RedisData, bool) {

	if len(cmd) == 0 {
		return resp.MakeErrorData("error: empty command"), false
	}

	_, isWriteCommand := WriteCommands[strings.ToLower(string(cmd[0]))]

	// 如果正在事务中
	if cli.inTx && NotTxCommand(strings.ToLower(string(cmd[0]))) {
		cli.tx = append(cli.tx, cli.cmd)
		cli.txRaw = append(cli.txRaw, cli.raw)
		return resp.MakeStringData("QUEUED"), false
	}

	f, ok := CommandTable[strings.ToLower(string(cmd[0]))]

	if !ok {

		return nil, isWriteCommand
	}

	return f(server, cli, cmd), isWriteCommand
}

func CheckCommandAndLength(cmd *[][]byte, name string, minLength int) (resp.RedisData, bool) {
	cmdName := strings.ToLower(string((*cmd)[0]))
	if cmdName != name {
		return resp.MakeErrorData("Server error"), false
	}

	if len(*cmd) < minLength {
		return resp.MakeErrorData(fmt.Sprintf("ERR wrong number of arguments for '%s' command", (*cmd)[1])), false
	}

	return nil, true
}

func NotTxCommand(cmd string) bool {
	return cmd != "exec" && cmd != "discard" && cmd != "watch" && cmd != "multi"
}
