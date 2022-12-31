package cmd

import (
	"MemTable/db"
	"MemTable/db/structure"
	"MemTable/resp"
	"strconv"
)

func hSet(db *db.DataBase, cmd [][]byte) resp.RedisData {
	e, ok := CheckCommandAndLength(&cmd, "hset", 4)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		value = structure.NewDict(12)
		db.SetKey(string(cmd[1]), value)
	}

	e = CheckType(value, HASH)
	if e != nil {
		return e
	}

	hashVal := value.(*structure.Dict)

	l := len(cmd)

	if l%2 == 1 {
		return resp.MakeErrorData("ERR wrong number of arguments for 'hset' command")
	}

	for i := 2; i < l; i += 2 {

		hashVal.Set(string(cmd[i]), cmd[i+1])

	}

	return resp.MakeIntData(int64(l/2 - 1))
}

func hMSet(db *db.DataBase, cmd [][]byte) resp.RedisData {
	e, ok := CheckCommandAndLength(&cmd, "hmset", 4)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		value = structure.NewDict(12)
		db.SetKey(string(cmd[1]), value)
	}

	e = CheckType(value, HASH)
	if e != nil {
		return e
	}

	hashVal := value.(*structure.Dict)

	l := len(cmd)

	if l%2 == 1 {
		return resp.MakeErrorData("ERR wrong number of arguments for 'hmset' command")
	}

	for i := 2; i < l; i += 2 {

		hashVal.Set(string(cmd[i]), cmd[i+1])

	}

	return resp.MakeStringData("OK")
}

func hGet(db *db.DataBase, cmd [][]byte) resp.RedisData {
	e, ok := CheckCommandAndLength(&cmd, "hget", 3)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	e = CheckType(value, HASH)
	if e != nil {
		return e
	}

	hashVal := value.(*structure.Dict)

	val, ok := hashVal.Get(string(cmd[2]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	return resp.MakeBulkData(val.([]byte))
}

func hMGet(db *db.DataBase, cmd [][]byte) resp.RedisData {
	e, ok := CheckCommandAndLength(&cmd, "hmget", 3)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeArrayData(nil)
	}

	e = CheckType(value, HASH)
	if e != nil {
		return e
	}

	hashVal := value.(*structure.Dict)

	res := make([]resp.RedisData, 0)

	for _, key := range cmd[2:] {

		val, ok := hashVal.Get(string(key))
		if ok {
			res = append(res, resp.MakeBulkData(val.([]byte)))
		}
	}

	return resp.MakeArrayData(res)
}

func hExists(db *db.DataBase, cmd [][]byte) resp.RedisData {
	e, ok := CheckCommandAndLength(&cmd, "hexists", 3)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(0)
	}

	e = CheckType(value, HASH)
	if e != nil {
		return e
	}

	hashVal := value.(*structure.Dict)

	_, ok = hashVal.Get(string(cmd[2]))
	if !ok {
		return resp.MakeIntData(0)
	}

	return resp.MakeIntData(1)
}

func hDel(db *db.DataBase, cmd [][]byte) resp.RedisData {
	e, ok := CheckCommandAndLength(&cmd, "hdel", 3)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(0)
	}

	e = CheckType(value, HASH)
	if e != nil {
		return e
	}

	hashVal := value.(*structure.Dict)

	deleted := 0

	for _, key := range cmd[2:] {
		if hashVal.Delete(string(key)) {
			deleted++
		}
	}

	return resp.MakeIntData(int64(deleted))
}

func hGetAll(db *db.DataBase, cmd [][]byte) resp.RedisData {
	e, ok := CheckCommandAndLength(&cmd, "hgetall", 2)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeArrayData(nil)
	}

	e = CheckType(value, HASH)
	if e != nil {
		return e
	}

	hashVal := value.(*structure.Dict)

	dicts, n := hashVal.GetAll()

	res := make([]resp.RedisData, n*2)

	i := 0
	for _, dict := range *dicts {
		for k, v := range dict {
			res[i] = resp.MakeBulkData([]byte(k))
			res[i+1] = resp.MakeBulkData(v.([]byte))
			i += 2
		}
	}
	return resp.MakeArrayData(res)
}

func hKeys(db *db.DataBase, cmd [][]byte) resp.RedisData {
	e, ok := CheckCommandAndLength(&cmd, "hkeys", 2)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeArrayData(nil)
	}

	e = CheckType(value, HASH)
	if e != nil {
		return e
	}

	hashVal := value.(*structure.Dict)

	keys, n := hashVal.Keys("")

	res := make([]resp.RedisData, n)

	for i, key := range keys {
		res[i] = resp.MakeBulkData([]byte(key))
	}
	return resp.MakeArrayData(res)
}

func hVals(db *db.DataBase, cmd [][]byte) resp.RedisData {

	e, ok := CheckCommandAndLength(&cmd, "hvals", 2)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeArrayData(nil)
	}

	e = CheckType(value, HASH)
	if e != nil {
		return e
	}

	hashVal := value.(*structure.Dict)

	dicts, n := hashVal.GetAll()

	res := make([]resp.RedisData, n)

	i := 0
	for _, dict := range *dicts {
		for _, v := range dict {
			res[i] = resp.MakeBulkData(v.([]byte))
			i++
		}
	}
	return resp.MakeArrayData(res)
}

func hIncrBy(db *db.DataBase, cmd [][]byte) resp.RedisData {
	e, ok := CheckCommandAndLength(&cmd, "hincrby", 4)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		value = structure.NewDict(12)
		db.SetKey(string(cmd[1]), value)
	}

	e = CheckType(value, HASH)
	if e != nil {
		return e
	}

	hashVal := value.(*structure.Dict)

	increment, err := strconv.Atoi(string(cmd[3]))
	if err != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	val, ok := hashVal.Get(string(cmd[2]))
	if !ok {
		val = cmd[3]
		hashVal.Set(string(cmd[2]), val)
		return resp.MakeIntData(int64(increment))
	}

	intVal, err := strconv.Atoi(string(val.([]byte)))
	if err != nil {
		return resp.MakeErrorData("ERR hash value is not an integer")
	}

	intVal += increment
	hashVal.Set(string(cmd[2]), []byte(strconv.Itoa(intVal)))

	return resp.MakeIntData(int64(intVal))
}

// func hIncrByFloat(db *db.DataBase, cmd [][]byte) resp.RedisData {}
func hLen(db *db.DataBase, cmd [][]byte) resp.RedisData {
	e, ok := CheckCommandAndLength(&cmd, "hlen", 2)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(0)
	}

	e = CheckType(value, HASH)
	if e != nil {
		return e
	}

	hashVal := value.(*structure.Dict)

	count := hashVal.Size()

	return resp.MakeIntData(int64(count))
}
func hStrLen(db *db.DataBase, cmd [][]byte) resp.RedisData {
	e, ok := CheckCommandAndLength(&cmd, "hstrlen", 3)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(0)
	}

	e = CheckType(value, HASH)
	if e != nil {
		return e
	}

	hashVal := value.(*structure.Dict)

	val, ok := hashVal.Get(string(cmd[2]))
	if !ok {
		return resp.MakeIntData(0)
	}

	sl := len(val.([]byte))

	return resp.MakeIntData(int64(sl))
}

func hRandField(db *db.DataBase, cmd [][]byte) resp.RedisData {
	e, ok := CheckCommandAndLength(&cmd, "hrandfield", 2)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeArrayData(nil)
	}

	e = CheckType(value, HASH)
	if e != nil {
		return e
	}

	hashVal := value.(*structure.Dict)

	count := 1

	if len(cmd) >= 3 {
		l, err := strconv.Atoi(string(cmd[2]))
		if err != nil {
			return resp.MakeErrorData("ERR value is not an integer or out of range")
		}
		count = l
	}

	selected := hashVal.RandomKeys(count)

	res := make([]resp.RedisData, len(selected))
	i := 0
	for key, _ := range selected {
		res[i] = resp.MakeBulkData([]byte(key))
		i++
	}
	return resp.MakeArrayData(res)
}

func RegisterHashCommands() {
	RegisterCommand("hset", hSet)
	RegisterCommand("hget", hGet)
	RegisterCommand("hexists", hExists)
	RegisterCommand("hdel", hDel)
	RegisterCommand("hmset", hMSet)
	RegisterCommand("hmget", hMGet)
	RegisterCommand("hgetall", hGetAll)
	RegisterCommand("hkeys", hKeys)
	RegisterCommand("hvals", hVals)
	RegisterCommand("hincrby", hIncrBy)
	RegisterCommand("hlen", hLen)
	RegisterCommand("hstrlen", hStrLen)
	RegisterCommand("hrandfield", hRandField)
}
