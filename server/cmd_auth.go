package server

import (
	"github.com/tangrc99/MemTable/resp"
)

func auth(server *Server, cli *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(cmd, "auth", 2)
	if !ok {
		return e
	}

	// this is an old version command
	if len(cmd) == 2 {
		user, exist := server.acl.FindUser("default")
		if !exist {

		}

		if !user.HasPassword() {
			return resp.MakeErrorData("ERR Client sent AUTH, but no password is set")
		}

		matched := user.IsPasswordMatch(string(cmd[1]))
		if !matched {
			return resp.MakeErrorData("ERR invalid password")
		}
		cli.user = user
		cli.auth = matched

	} else {
		user, exist := server.acl.FindUser(string(cmd[2]))
		if !exist {

		}

		if !user.HasPassword() {
			return resp.MakeErrorData("ERR Client sent AUTH, but no password is set")
		}

		matched := user.IsPasswordMatch(string(cmd[1]))
		if !matched {
			return resp.MakeErrorData("ERR invalid password")
		}
		cli.user = user
		cli.auth = matched
	}

	return resp.MakeStringData("OK")
}

func registerAuthCommands() {
	RegisterCommand("auth", auth, RD)
}
