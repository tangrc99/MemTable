package cmd

import (
	"fmt"
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/db/structure"
	"github.com/tangrc99/MemTable/logger"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/global"
	"strconv"
)

// del 删除多个键，并返回删除数量
func del(db *db.DataBase, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "del", 2)
	if !ok {
		return e
	}

	deleted := 0

	for _, key := range cmd[1:] {
		if db.DeleteKey(string(key)) {
			deleted++
		}
	}

	return resp.MakeIntData(int64(deleted))
}

func dump(db *db.DataBase, cmd [][]byte) resp.RedisData {

	return resp.MakeErrorData("")
}

// exists 检查多个键是否存在，返回存在数量
func exists(db *db.DataBase, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "exists", 2)
	if !ok {
		return e
	}

	exist := 0

	for _, key := range cmd[1:] {

		if db.ExistKey(string(key)) {
			exist++
		}
	}

	return resp.MakeIntData(int64(exist))
}

func expire(db *db.DataBase, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "expire", 3)
	if !ok {
		return e
	}

	period, err := strconv.ParseInt(string(cmd[2]), 10, 64)
	if err != nil {
		logger.Error("expire Function: cmd[2] %s is not int", string(cmd[2]))
		return resp.MakeErrorData(fmt.Sprintf("error: %s is not int", string(cmd[2])))
	}

	tp := global.Now.Unix() + period

	ok = db.SetTTL(string(cmd[1]), tp)

	if ok {
		return resp.MakeIntData(1)
	}
	return resp.MakeIntData(0)
}

func expireAt(db *db.DataBase, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "expireat", 3)
	if !ok {
		return e
	}

	tp, err := strconv.ParseInt(string(cmd[2]), 10, 64)
	if err != nil {
		logger.Error("expire Function: cmd[2] %s is not int", string(cmd[2]))
		return resp.MakeErrorData(fmt.Sprintf("error: %s is not int", string(cmd[2])))
	}

	ok = db.SetTTL(string(cmd[1]), tp)

	if ok {
		return resp.MakeIntData(1)
	}
	return resp.MakeIntData(0)
}

func pExpire(db *db.DataBase, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "pexpire", 3)
	if !ok {
		return e
	}

	period, err := strconv.ParseInt(string(cmd[2]), 10, 64)
	if err != nil {
		logger.Error("expire Function: cmd[2] %s is not int", string(cmd[2]))
		return resp.MakeErrorData(fmt.Sprintf("error: %s is not int", string(cmd[2])))
	}

	tp := global.Now.Unix() + period/1000

	ok = db.SetTTL(string(cmd[1]), tp)

	if ok {
		return resp.MakeIntData(1)
	}
	return resp.MakeIntData(0)
}

func pExpireAt(db *db.DataBase, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "pexpireat", 3)
	if !ok {
		return e
	}

	tp, err := strconv.ParseInt(string(cmd[2]), 10, 64)
	if err != nil {
		logger.Error("expire Function: cmd[2] %s is not int", string(cmd[2]))
		return resp.MakeErrorData(fmt.Sprintf("error: %s is not int", string(cmd[2])))
	}

	ok = db.SetTTL(string(cmd[1]), tp)

	if ok {
		return resp.MakeIntData(1)
	}
	return resp.MakeIntData(0)
}

// keys 返回所有键，首行为个数
func keys(db *db.DataBase, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	err, ok := checkCommandAndLength(&cmd, "keys", 1)
	if !ok {
		return err
	}

	pattern := ""
	if len(cmd) == 2 {
		pattern = string(cmd[1])
	}

	ks, size := db.KeysByte(pattern)

	res := make([]resp.RedisData, size+1)
	res[0] = resp.MakeIntData(int64(size))

	//res[0] = resp.MakeBulkData([]byte(string(rune(size))))
	for i := 1; i < size+1; i++ {
		res[i] = resp.MakeBulkData(ks[i-1])
	}

	return resp.MakeArrayData(res)
}

func ttl(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	err, ok := checkCommandAndLength(&cmd, "ttl", 2)
	if !ok {
		return err
	}

	tp := db.GetTTL(string(cmd[1]))

	return resp.MakeIntData(tp)
}

func randomKey(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	err, ok := checkCommandAndLength(&cmd, "randomkey", 1)
	if !ok {
		return err
	}

	key, ok := db.RandomKey()
	if !ok {
		return resp.MakeStringData("nil")
	}

	return resp.MakeBulkData([]byte(key))
}

func rename(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	err, ok := checkCommandAndLength(&cmd, "rename", 3)
	if !ok {
		return err
	}

	oldKey := string(cmd[1])
	newKey := string(cmd[2])

	ok = db.RenameKey(oldKey, newKey)
	if !ok {
		return resp.MakeErrorData("error: no such key")
	}

	return resp.MakeStringData("OK")
}

func renameNX(db *db.DataBase, cmd [][]byte) resp.RedisData {

	return resp.MakeErrorData("")
}

func typeKey(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	err, ok := checkCommandAndLength(&cmd, "type", 2)
	if !ok {
		return err
	}

	value, ok := db.GetKey(string(cmd[1]))

	typeName := ""

	if !ok {
		typeName = "none"
	} else {

		if _, ok := value.([]byte); ok {
			typeName = "string"
		} else if _, ok := value.(*structure.List); ok {
			typeName = "list"
		} else if _, ok := value.(*structure.Dict); ok {
			typeName = "hash"
		} else if _, ok := value.(*structure.Set); ok {
			typeName = "set"
		} else if _, ok := value.(*structure.ZSet); ok {
			typeName = "zset"
		}
	}

	return resp.MakeStringData(typeName)
}

func registerKeyCommands() {

	registerCommand("del", del, WR)
	registerCommand("exists", exists, RD)
	registerCommand("keys", keys, RD)
	registerCommand("ttl", ttl, RD)
	registerCommand("expire", expire, RD)
	//registerCommand("expireat", expireAt)
	registerCommand("pexpire", pExpire, RD)
	//registerCommand("pexpireat", pExpireAt)
	registerCommand("rename", rename, WR)
	registerCommand("type", typeKey, RD)
	registerCommand("randomkey", randomKey, RD)
}
