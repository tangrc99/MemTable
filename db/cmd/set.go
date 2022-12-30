package cmd

import (
	"MemTable/db"
	"MemTable/db/structure"
	"MemTable/resp"
)

func sadd(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "sadd", 3)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))

	if !ok {
		set := structure.NewSet()
		set.Add(string(cmd[2]))
		db.SetKey(string(cmd[1]), set)
		return resp.MakeStringData("OK")
	}

	// 进行类型检查，会自动检查过期选项
	if err := CheckType(value, SET); err != nil {
		return err
	}

	value.(*structure.Set).Add(string(cmd[2]))

	// 重置 TTL
	db.RemoveTTL(string(cmd[1]))

	return resp.MakeStringData("OK")

}

func sRem(db *db.DataBase, cmd [][]byte) resp.RedisData {}

func scard(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "scard", 2)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(0)
	}

	if err := CheckType(value, SET); err != nil {
		return err
	}

	return resp.MakeIntData(int64(value.(*structure.Set).Size()))
}

func sismember(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "sismember", 2)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))

	if !ok {
		return resp.MakeIntData(0)

	}

	// 进行类型检查，会自动检查过期选项
	if err := CheckType(value, SET); err != nil {
		return err
	}

	set, _ := db.GetKey(string(cmd[1]))
	exist := set.(*structure.Set).Exist(string(cmd[2]))
	if !exist {
		return resp.MakeIntData(0)
	}

	return resp.MakeIntData(1)
}

func sMembers(db *db.DataBase, cmd [][]byte) resp.RedisData {}

func sDiff(db *db.DataBase, cmd [][]byte) resp.RedisData {}

func sDiffStore(db *db.DataBase, cmd [][]byte) resp.RedisData {}

func sInter(db *db.DataBase, cmd [][]byte) resp.RedisData {}

func sInterStore(db *db.DataBase, cmd [][]byte) resp.RedisData {}

func sMove(db *db.DataBase, cmd [][]byte) resp.RedisData {}

func sPop(db *db.DataBase, cmd [][]byte) resp.RedisData {}

func sRandMember(db *db.DataBase, cmd [][]byte) resp.RedisData {}

func sUnion(db *db.DataBase, cmd [][]byte) resp.RedisData {}

func sUnionStore(db *db.DataBase, cmd [][]byte) resp.RedisData {}

func sScan(db *db.DataBase, cmd [][]byte) resp.RedisData {}

func RegisterSetCommands() {
	RegisterCommand("sadd", sadd)
	RegisterCommand("scard", scard)
	RegisterCommand("sismember", sismember)
}
