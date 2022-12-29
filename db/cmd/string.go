package cmd

import (
	"MemTable/db"
	"MemTable/resp"
)

func set(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "set", 3)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))

	// 进行类型检查，会自动检查过期选项
	if err := CheckType(value, STRING); err != nil {
		return err
	}

	// 键值对设置
	db.SetKey(string(cmd[1]), string(cmd[2]))

	// 重置 TTL
	db.RemoveTTL(string(cmd[1]))

	return resp.MakeStringData("OK")
}

func get(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "get", 2)
	if !ok {
		return e
	}

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

func strlen(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "strlen", 2)
	if !ok {
		return e
	}

	// 检查 key 是否被设置为 ttl，如果设置需要删除
	value, ok := db.GetKey(string(cmd[1]))

	if !ok {
		return resp.MakeIntData(-1)
	}

	if err := CheckType(value, STRING); err != nil {
		return err
	}

	strVal, ok := value.(string)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	return resp.MakeIntData(int64(len(strVal)))
}

func getRange(db *db.DataBase, cmd [][]byte) resp.RedisData {

	return resp.MakeStringData("")
}
func mget(db *db.DataBase, cmd [][]byte) resp.RedisData {
	return resp.MakeArrayData(nil)
}

func mset(db *db.DataBase, cmd [][]byte) resp.RedisData {
	return resp.MakeStringData("OK")
}

func incr(db *db.DataBase, cmd [][]byte) resp.RedisData {

	return resp.MakeStringData("")
}

func incrby(db *db.DataBase, cmd [][]byte) resp.RedisData {
	return resp.MakeStringData("")

}

func decr(db *db.DataBase, cmd [][]byte) resp.RedisData {
	return resp.MakeStringData("")

}
func decrby(db *db.DataBase, cmd [][]byte) resp.RedisData {
	return resp.MakeStringData("")

}

func append(db *db.DataBase, cmd [][]byte) resp.RedisData {
	return resp.MakeStringData("")
}

func RegisterStringCommands() {

	RegisterCommand("set", set)
	RegisterCommand("get", get)
	RegisterCommand("strlen", strlen)

}
