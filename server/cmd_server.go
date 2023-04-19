package server

import (
	"fmt"
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/resp"
	"os"
	"path"
	"strconv"
	"strings"
	"syscall"
)

func save(server *Server, _ *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(cmd, "save", 1)
	if !ok {
		return e
	}

	if len(cmd) == 2 {
		return resp.MakeErrorData("ERR wrong number of arguments for 'save' command")

	}

	server.RDB(path.Join(server.dir, server.rdbFile))

	return resp.MakeStringData("OK")
}

func bgsave(server *Server, _ *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(cmd, "bgsave", 1)
	if !ok {
		return e
	}

	if len(cmd) == 2 {
		return resp.MakeErrorData("ERR wrong number of arguments for 'save' command")

	}

	server.BGRDB()

	return resp.MakeStringData("Background saving started")
}

func shutdown(server *Server, cli *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(cmd, "shutdown", 1)
	if !ok {
		return e
	}

	server.clis.RemoveClient(cli)

	err := syscall.Kill(os.Getpid(), syscall.SIGINT)

	if err != nil {
		return resp.MakeErrorData("ERR shutdown failed")
	}

	return nil
}

func flushdb(server *Server, cli *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(cmd, "flushdb", 1)
	if !ok {
		return e
	}

	//TODO: 异步操作
	server.dbs[cli.dbSeq].ReviseNotifyAll()
	server.dbs[cli.dbSeq] = db.NewDataBase(slotNum)

	return resp.MakeStringData("OK")
}

func flushall(server *Server, _ *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(cmd, "flushall", 1)
	if !ok {
		return e
	}

	for i := 0; i < server.dbNum; i++ {
		server.dbs[i].ReviseNotifyAll()
		server.dbs[i] = db.NewDataBase(slotNum)
	}

	return resp.MakeStringData("OK")
}

func dbsize(server *Server, cli *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(cmd, "dbsize", 1)
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

func slowlog(server *Server, _ *Client, cmd [][]byte) resp.RedisData {

	e, ok := CheckCommandAndLength(cmd, "slowlog", 2)
	if !ok {
		return e
	}

	subcommand := strings.ToLower(string(cmd[1]))

	switch subcommand {
	case "len":

		return resp.MakeIntData(server.slowlog.Len())

	case "get":

		if len(cmd) < 3 {
			return resp.MakeErrorData("ERR wrong number of arguments for 'slowlog get' command")
		}

		limit, err := strconv.Atoi(string(cmd[2]))
		if err != nil {
			return resp.MakeErrorData("ERR value is not an integer or out of range")
		}
		return server.slowlog.getEntries(limit)

	case "reset":

		server.slowlog.clear()

		return resp.MakeStringData("OK")
	}

	return resp.MakeErrorData(fmt.Sprintf("ERR unknown subcommand '%s' of slowlog", subcommand))
}

// info 用于显示服务器的状态，命令格式： info [section]
func info(server *Server, _ *Client, cmd [][]byte) resp.RedisData {

	e, ok := CheckCommandAndLength(cmd, "info", 1)
	if !ok {
		return e
	}

	section := ""

	if len(cmd) == 2 {
		section = string(cmd[1])
	}

	return resp.MakeStringData(server.Information(section))
}

func registerServerCommand() {
	RegisterCommand("shutdown", shutdown, RD)
	RegisterCommand("flushdb", flushdb, WR)
	RegisterCommand("flushall", flushall, WR)
	RegisterCommand("dbsize", dbsize, RD)
	RegisterCommand("save", save, RD)
	RegisterCommand("bgsave", bgsave, RD)
	RegisterCommand("slowlog", slowlog, RD)
	RegisterCommand("info", info, RD)
}
