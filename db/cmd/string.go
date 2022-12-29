package cmd

import (
	"MemTable/db"
	"MemTable/resp"
	"strings"
)

func Set(db *db.DataBase, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "set" {
		return resp.MakeErrorData("Server error")
	}

	if len(cmd) < 3 {
		return resp.MakeErrorData("error: commands is invalid")
	}

	// 进行类型检查，会自动检查过期选项
	r, ok := CheckOldType(db, string(cmd[1]), STRING)
	if ok == MISMATCH {
		return r
	}

	// 键值对设置
	db.SetKey(string(cmd[1]), string(cmd[2]))

	// 重置 TTL
	db.RemoveTTL(string(cmd[1]))

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
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	return resp.MakeBulkData([]byte(strVal))
}

func StrLen(db *db.DataBase, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	println(cmdName)

	if cmdName != "strlen" {
		return resp.MakeErrorData("Server error")
	}

	// 检查 key 是否被设置为 ttl，如果设置需要删除
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(-1)
	}

	strVal, ok := value.(string)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	return resp.MakeIntData(int64(len(strVal)))
}

func RegisterStringCommands() {

	RegisterCommand("set", Set)
	RegisterCommand("get", Get)
	RegisterCommand("strlen", StrLen)

}
