package cmd

import (
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/resp"
	"strconv"
)

func set(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "set", 3)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))

	// 进行类型检查，会自动检查过期选项
	if err := checkType(value, STRING); err != nil {
		return err
	}

	// 键值对设置
	db.SetKey(string(cmd[1]), cmd[2])

	// 重置 TTL
	db.RemoveTTL(string(cmd[1]))

	return resp.MakeStringData("OK")
}

func get(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "get", 2)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	byteVal, ok := value.([]byte)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	return resp.MakeBulkData(byteVal)
}

func getset(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "getset", 3)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	err := checkType(value, STRING)
	if err != nil {
		return err
	}

	// 重置 TTL
	db.SetKey(string(cmd[1]), cmd[2])
	db.RemoveTTL(string(cmd[1]))
	return resp.MakeStringData("OK")
}

func strlen(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "strlen", 2)
	if !ok {
		return e
	}

	// 检查 key 是否被设置为 ttl，如果设置需要删除
	value, ok := db.GetKey(string(cmd[1]))

	if !ok {
		return resp.MakeIntData(-1)
	}

	if err := checkType(value, STRING); err != nil {
		return err
	}

	strVal, ok := value.([]byte)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	return resp.MakeIntData(int64(len(strVal)))
}

func getRange(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "getrange", 4)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	byteVal, ok := value.([]byte)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	start, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}
	end, err := strconv.Atoi(string(cmd[3]))
	if err != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	l := len(byteVal)

	if start > end || start >= l || end < 0 {
		return resp.MakeBulkData([]byte{})
	}

	if end > l {
		end = l
	}
	return resp.MakeBulkData(byteVal[start:end])
}

func setRange(db *db.DataBase, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "setrange", 4)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	byteVal, ok := value.([]byte)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	start, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	ol := len(byteVal)
	l := start + len(cmd[3])
	if l < ol {
		l = ol
	}

	newVal := make([]byte, l)
	for i, c := range byteVal {
		newVal[i] = c
	}
	for i, c := range cmd[3] {
		newVal[start+i] = c
	}

	db.SetKey(string(cmd[1]), newVal)

	return resp.MakeIntData(int64(l))
}

// mget 如果有对应的键值，但是类型不匹配，会返回 nil
func mget(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "mget", 2)
	if !ok {
		return e
	}

	res := make([]resp.RedisData, len(cmd)-1)

	for i, key := range cmd[1:] {

		value, ok := db.GetKey(string(key))
		if !ok {
			res[i] = resp.MakeStringData("nil")

		} else {

			byteVal, ok := value.([]byte)
			if !ok {
				res[i] = resp.MakeStringData("nil")
			}

			res[i] = resp.MakeBulkData(byteVal)
		}

	}

	return resp.MakeArrayData(res)
}

func mset(db *db.DataBase, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "mset", 3)
	if !ok {
		return e
	}

	if len(cmd)%2 == 0 {
		return resp.MakeErrorData("ERR wrong number of arguments for 'mset' command")
	}

	for i := 1; i < len(cmd); i += 2 {
		db.SetKey(string(cmd[i]), cmd[i+1])
		// 重置 TTL
		db.RemoveTTL(string(cmd[i]))
	}

	return resp.MakeStringData("OK")
}

func incr(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "incr", 2)
	if !ok {
		return e
	}
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	byteVal, ok := value.([]byte)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	intVal, err := strconv.Atoi(string(byteVal))
	if err != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	intVal++
	db.SetKey(string(cmd[1]), []byte(strconv.Itoa(intVal)))

	return resp.MakeIntData(int64(intVal))
}

func incrby(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "incrby", 3)
	if !ok {
		return e
	}
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	byteVal, ok := value.([]byte)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	intVal, err := strconv.Atoi(string(byteVal))
	if err != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	increment, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	intVal += increment
	db.SetKey(string(cmd[1]), []byte(strconv.Itoa(intVal)))

	return resp.MakeIntData(int64(intVal))
}

func decr(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "decr", 2)
	if !ok {
		return e
	}
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	byteVal, ok := value.([]byte)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	intVal, err := strconv.Atoi(string(byteVal))
	if err != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	intVal--
	db.SetKey(string(cmd[1]), []byte(strconv.Itoa(intVal)))

	return resp.MakeIntData(int64(intVal))

}

func decrby(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "decrby", 3)
	if !ok {
		return e
	}
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	byteVal, ok := value.([]byte)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	intVal, err := strconv.Atoi(string(byteVal))
	if err != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	decrement, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	intVal -= decrement
	db.SetKey(string(cmd[1]), []byte(strconv.Itoa(intVal)))

	return resp.MakeIntData(int64(intVal))

}

func appendStr(db *db.DataBase, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "append", 3)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	byteVal, ok := value.([]byte)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	byteVal = append(byteVal, cmd[2]...)

	db.SetKey(string(cmd[1]), byteVal)

	return resp.MakeIntData(int64(len(byteVal)))
}

func registerStringCommands() {

	registerCommand("set", set, WR)
	registerCommand("get", get, RD)
	registerCommand("getset", getset, WR)
	registerCommand("strlen", strlen, RD)
	registerCommand("getrange", getRange, RD)
	registerCommand("setrange", setRange, WR)
	registerCommand("mget", mget, RD)
	registerCommand("mset", mset, WR)
	registerCommand("incr", incr, WR)
	registerCommand("incrby", incrby, WR)
	registerCommand("decr", decr, WR)
	registerCommand("decrby", decrby, WR)
	registerCommand("append", appendStr, WR)

}
