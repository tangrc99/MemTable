package server

import (
	"MemTable/db"
	"MemTable/resp"
	"os"
	"strconv"
	"syscall"
)

func save(server *Server, _ *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "save", 1)
	if !ok {
		return e
	}

	if len(cmd) == 2 {
		return resp.MakeErrorData("ERR wrong number of arguments for 'save' command")

	}

	server.RDB("dump.rdb")

	return resp.MakeStringData("OK")
}

func bgsave(server *Server, cli *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "bgsave", 1)
	if !ok {
		return e
	}

	if len(cmd) == 2 {
		return resp.MakeErrorData("ERR wrong number of arguments for 'save' command")

	}

	server.BGRDB()

	return resp.MakeStringData("Background saving started")
}

func shutdown(_ *Server, _ *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "shutdown", 1)
	if !ok {
		return e
	}

	err := syscall.Kill(os.Getpid(), syscall.SIGINT)

	if err != nil {
		return resp.MakeErrorData("ERR shutdown failed")
	}

	return resp.MakeStringData("")
}

func flushdb(server *Server, cli *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "flushdb", 1)
	if !ok {
		return e
	}

	//FIXME: 异步操作
	server.dbs[cli.dbSeq].ReviveNotifyAll()
	server.dbs[cli.dbSeq] = db.NewDataBase()

	return resp.MakeStringData("OK")
}

func flushall(server *Server, _ *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "flushall", 1)
	if !ok {
		return e
	}

	for i := 0; i < server.dbNum; i++ {
		server.dbs[i].ReviveNotifyAll()
		server.dbs[i] = db.NewDataBase()
	}

	return resp.MakeStringData("OK")
}

func dbsize(server *Server, cli *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "dbsize", 1)
	if !ok {
		return e
	}

	if len(cmd) == 2 {

		dbSeq, err := strconv.Atoi(string(cmd[1]))
		if err != nil {
			return resp.MakeErrorData("ERR value is not an integer or out of range")
		}

		if dbSeq >= server.dbNum {
			return resp.MakeErrorData("ERR DB index is out of range")
		}

		size := server.dbs[dbSeq].Size()
		return resp.MakeIntData(int64(size))
	}
	size := server.dbs[cli.dbSeq].Size()
	return resp.MakeIntData(int64(size))
}

func RegisterServerCommand() {
	RegisterCommand("shutdown", shutdown, RD)
	RegisterCommand("flushdb", flushdb, WR)
	RegisterCommand("flushall", flushall, WR)
	RegisterCommand("dbsize", dbsize, RD)
	RegisterCommand("save", save, RD)
	RegisterCommand("bgsave", bgsave, RD)
}
