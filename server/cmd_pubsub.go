package server

import (
	"github.com/tangrc99/MemTable/resp"
)

func publish(server *Server, _ *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "publish", 3)
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
	e, ok := CheckCommandAndLength(&cmd, "subscribe", 2)
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
	e, ok := CheckCommandAndLength(&cmd, "subscribe", 2)
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

func registerPubSubCommands() {
	RegisterCommand("publish", publish, RD)
	RegisterCommand("subscribe", subscribe, RD)
	RegisterCommand("unsubscribe", unsubscribe, RD)
}
