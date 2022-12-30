package server

import (
	"MemTable/resp"
	"fmt"
	"strings"
)

type Command = func(server *Server, cli *Client, cmd [][]byte) resp.RedisData

var CommandTable = make(map[string]Command)

func RegisterCommand(name string, cmd Command) {
	CommandTable[name] = cmd
}

func init() {
	RegisterPubSubCommands()
}

func ExecCommand(server *Server, cli *Client, cmd [][]byte) resp.RedisData {

	if len(cmd) == 0 {
		return resp.MakeErrorData("error: empty command")
	}

	f, ok := CommandTable[strings.ToLower(string(cmd[0]))]
	if !ok {
		return nil
	}

	return f(server, cli, cmd)
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
