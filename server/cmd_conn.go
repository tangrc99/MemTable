package server

import (
	"MemTable/resp"
	"strconv"
)

func ping(server *Server, cli *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "ping", 1)
	if !ok {
		return e
	}

	return resp.MakeStringData("pong")
}

func quit(server *Server, cli *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "quit", 1)
	if !ok {
		return e
	}

	server.clis.RemoveClient(cli)

	return resp.MakeStringData("")
}

func selectDB(server *Server, cli *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "select", 2)
	if !ok {
		return e
	}

	dbSeq, err := strconv.Atoi(string(cmd[1]))
	if err != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	if dbSeq > server.dbNum {
		return resp.MakeErrorData("ERR DB index is out of range")
	}

	cli.dbSeq = dbSeq
	cli.db = server.dbs[dbSeq]

	return resp.MakeStringData("OK")
}

func RegisterConnectionCommands() {
	RegisterCommand("ping", ping)
	RegisterCommand("quit", quit)
	RegisterCommand("select", selectDB)
}
