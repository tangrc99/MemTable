package server

import (
	"fmt"
	"github.com/tangrc99/MemTable/resp"
	"strconv"
)

func multi(_ *Server, cli *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(cmd, "multi", 1)
	if !ok {
		return e
	}

	if cli.inTx {
		return resp.MakeErrorData("ERR MULTI calls can not be nested")
	}

	cli.InitTX()

	return resp.MakeStringData("OK")
}

func execTX(server *Server, cli *Client, cmds [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(cmds, "exec", 1)
	if !ok {
		return e
	}

	if !cli.inTx {
		return resp.MakeErrorData("ERR EXEC without MULTI")
	}

	defer func() {
		cli.inTx = false
		cli.tx = make([][][]byte, 0)

		for dbSeq, keys := range cli.watched {
			for _, key := range keys {
				server.dbs[dbSeq].UnWatch(key, &cli.revised)
			}
		}
		cli.ClearWatchers()
	}()

	if cli.revised {

		return resp.MakeStringData("nil")
	}

	cli.inTx = false

	reses := make([]resp.RedisData, len(cli.tx))

	for i, c := range cli.tx {

		// 执行服务命令
		res, isWriteCommand := ExecCommand(server, cli, c, nil)

		// 写命令需要完成aof持久化
		if isWriteCommand && server.aof != nil {

			if cli.dbSeq != 0 {
				// 多数据库场景需要加入数据库选择语句
				dbStr := strconv.Itoa(cli.dbSeq)
				server.aof.append([]byte(fmt.Sprintf("*2\r\n$6\r\nselect\r\n$%d\r\n%s\r\n", len(dbStr), dbStr)))
			}
			server.aof.append(cli.txRaw[i])
		}

		reses[i] = res

		if fmt.Sprintf("%T", res) == "*resp.ErrorData" {

			return resp.MakeArrayData(reses[0 : i+1])
		}
	}

	return resp.MakeArrayData(reses)
}

func watch(server *Server, cli *Client, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	e, ok := CheckCommandAndLength(cmd, "watch", 2)
	if !ok {
		return e
	}

	if cli.inTx {
		return resp.MakeErrorData("ERR WATCH inside MULTI is not allowed")
	}

	cli.InitWatchers()

	for _, key := range cmd[1:] {

		cli.watched[cli.dbSeq] = append(cli.watched[cli.dbSeq], string(key))
		server.dbs[cli.dbSeq].Watch(string(key), &cli.revised)
	}

	return resp.MakeStringData("OK")
}

func discard(_ *Server, cli *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(cmd, "discard", 1)
	if !ok {
		return e
	}

	if !cli.inTx {
		return resp.MakeErrorData("ERR DISCARD without MULTI")
	}

	cli.inTx = false
	cli.tx = make([][][]byte, 0)
	cli.watched = make(map[int][]string)
	cli.revised = false
	return resp.MakeStringData("OK")
}

func registerTransactionCommand() {
	RegisterCommand("multi", multi, RD)
	RegisterCommand("exec", execTX, RD)
	RegisterCommand("discard", discard, RD)
	RegisterCommand("watch", watch, RD)
}
