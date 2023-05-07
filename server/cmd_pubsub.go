package server

import (
	"github.com/tangrc99/MemTable/db/structure"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/global"
	"strconv"
)

func publish(server *Server, _ *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(cmd, "publish", 3)
	if !ok {
		return e
	}

	msg := make([]resp.RedisData, 3)
	msg[0] = resp.MakeBulkData([]byte("message"))
	msg[1] = resp.MakeBulkData(cmd[1])
	msg[2] = resp.MakeBulkData(cmd[2])

	notified := server.Chs.Publish(string(cmd[1]), resp.MakeArrayData(msg).ToBytes())

	return resp.MakeIntData(int64(notified))
}

func subscribe(server *Server, cli *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(cmd, "subscribe", 2)
	if !ok {
		return e
	}

	res := make([]resp.RedisData, (len(cmd)-1)*3)

	for i, channel := range cmd[1:] {
		subscribed := cli.Subscribe(server.Chs, string(channel))
		res[i*3] = resp.MakeIntData(int64(subscribed))
		res[i*3+1] = resp.MakeBulkData([]byte("subscribe"))
		res[i*3+2] = resp.MakeBulkData(channel)
	}
	return resp.MakeArrayData(res)
}

func unsubscribe(server *Server, cli *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(cmd, "unsubscribe", 2)
	if !ok {
		return e
	}
	subscribed := cli.UnSubscribe(server.Chs, string(cmd[1]))

	res := make([]resp.RedisData, 3)
	res[0] = resp.MakeIntData(int64(subscribed))
	res[1] = resp.MakeBulkData([]byte("unsubscribe"))
	res[2] = resp.MakeBulkData(cmd[1])

	return resp.MakeArrayData(res)
}

func bLPop(server *Server, cli *Client, cmd [][]byte) resp.RedisData {

	dataBase := server.dbs[cli.dbSeq]

	e, ok := CheckCommandAndLength(cmd, "blpop", 3)
	if !ok {
		return e
	}

	for i := 1; i < len(cmd)-1; i++ {
		value, ok := dataBase.GetKey(string(cmd[i]))
		if !ok {
			continue
		}
		// 如果可以取出，则直接取出
		listVal, ok := value.(*structure.List)
		if !ok {
			return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if !listVal.Empty() {
			v, _ := listVal.PopFront().(structure.Slice)
			return resp.MakeBulkData(v)
		}
	}

	timeout, w := strconv.Atoi(string(cmd[len(cmd)-1]))
	if w != nil {
		return resp.MakeErrorData("ERR timeout is not an integer or out of range")
	}
	deadline := global.Now.Unix() + int64(timeout)
	if timeout == 0 {
		deadline = -1
	}
	for i := 1; i < len(cmd)-1; i++ {
		dataBase.RegisterBlocked(string(cmd[i]), cli.id, cli.msg, deadline)
	}

	cli.blocked = true
	return nil
}

func bRPop(server *Server, cli *Client, cmd [][]byte) resp.RedisData {

	dataBase := server.dbs[cli.dbSeq]

	e, ok := CheckCommandAndLength(cmd, "brpop", 3)
	if !ok {
		return e
	}

	for i := 1; i < len(cmd)-1; i++ {
		value, ok := dataBase.GetKey(string(cmd[i]))
		if !ok {
			continue
		}
		// 如果可以取出，则直接取出
		listVal, ok := value.(*structure.List)
		if !ok {
			return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if !listVal.Empty() {
			v, _ := listVal.PopBack().(structure.Slice)
			return resp.MakeBulkData(v)
		}
	}

	timeout, w := strconv.Atoi(string(cmd[len(cmd)-1]))
	if w != nil {
		return resp.MakeErrorData("ERR timeout is not an integer or out of range")
	}
	deadline := global.Now.Unix() + int64(timeout)
	if timeout == 0 {
		deadline = -1
	}
	for i := 1; i < len(cmd)-1; i++ {
		dataBase.RegisterBlocked(string(cmd[i]), cli.id, cli.msg, deadline)
	}

	cli.blocked = true
	return nil
}

func registerPubSubCommands() {
	RegisterCommand("publish", publish, RD)
	RegisterCommand("subscribe", subscribe, RD)
	RegisterCommand("unsubscribe", unsubscribe, RD)
	RegisterCommand("blpop", bLPop, RD)
	RegisterCommand("brpop", bRPop, RD)
}
