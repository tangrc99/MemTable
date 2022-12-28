package cmd

import (
	"MemTable/db"
	"MemTable/resp"
	"strings"
)

func Set(db *db.DataBase, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "set" {
		return resp.MakeErrorData("Server error")
	}

	if len(cmd) < 3 {
		return resp.MakeErrorData("error: commands is invalid")
	}

	// 检查 key 是否被设置为 ttl，如果设置需要删除

	db.RemoveTTL(string(cmd[1]))

	db.SetKey(string(cmd[1]), string(cmd[2]))

	return resp.MakeStringData("OK")
}

func Get(db *db.DataBase, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	println(cmdName)

	if cmdName != "get" {
		return resp.MakeErrorData("Server error")
	}

	if len(cmd) < 2 {
		return resp.MakeErrorData("error: commands is invalid")
	}

	// 检查 key 是否被设置为 ttl，如果设置需要删除
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeStringData("nil")

	}

	strVal, ok := value.(string)
	if !ok {
		println("convert error")
	}
	byteVal := []byte(strVal)

	return resp.MakeBulkData(byteVal)
}
