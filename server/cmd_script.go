package server

import (
	"github.com/tangrc99/MemTable/resp"
	"strconv"
)

func evalCommand(_ *Server, cli *Client, cmd [][]byte) resp.RedisData {

	e, ok := CheckCommandAndLength(&cmd, "eval", 3)
	if !ok {
		return e
	}

	env.caller = cli

	body := string(cmd[1])

	keyNum, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return resp.MakeErrorData("ERR numkeys is not an integer or out of range")
	} else if keyNum > len(cmd) {
		return resp.MakeErrorData("ERR Number of keys can't be greater than number of args")
	} else if keyNum < 0 {
		return resp.MakeErrorData("ERR Number of keys can't be negative")
	}

	return evalGenericCommand(env.l, body, "", cmd[3:3+keyNum], cmd[3+keyNum:])
}

func evalShaCommand(_ *Server, cli *Client, cmd [][]byte) resp.RedisData {

	e, ok := CheckCommandAndLength(&cmd, "evalsha", 3)
	if !ok {
		return e
	}

	env.caller = cli
	sha := string(cmd[1])

	keyNum, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return resp.MakeErrorData("ERR numkeys is not an integer or out of range")
	} else if keyNum > len(cmd) {
		return resp.MakeErrorData("ERR Number of keys can't be greater than number of args")
	} else if keyNum < 0 {
		return resp.MakeErrorData("ERR Number of keys can't be negative")
	}

	// 这里需要更改 client 的命令，变为 eval 形式

	return evalGenericCommand(env.l, "", sha, cmd[3:3+keyNum], cmd[3+keyNum:])
}

func RegisterScriptCommands() {
	RegisterCommand("eval", evalCommand, WR)
	RegisterCommand("eval", evalShaCommand, WR)
}
