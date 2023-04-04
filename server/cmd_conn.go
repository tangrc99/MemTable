package server

import (
	"github.com/tangrc99/MemTable/resp"
	"strconv"
)

func ping(_ *Server, _ *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "ping", 1)
	if !ok {
		return e
	}

	if len(cmd) == 2 {
		return resp.MakeBulkData(cmd[1])
	}

	return resp.MakeStringData(string([]byte("pong")))
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

	if dbSeq >= server.dbNum {
		return resp.MakeErrorData("ERR DB index is out of range")
	}

	cli.dbSeq = dbSeq

	return resp.MakeStringData("OK")
}

func registerConnectionCommands() {
	RegisterCommand("ping", ping, RD)
	RegisterCommand("quit", quit, RD)
	RegisterCommand("select", selectDB, RD)
}
