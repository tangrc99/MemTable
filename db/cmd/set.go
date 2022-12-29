package cmd

import (
	"MemTable/db"
	"MemTable/db/structure"
	"MemTable/resp"
	"strings"
)

func SADD(db *db.DataBase, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "sadd" {
		return resp.MakeErrorData("Server error")
	}

	if len(cmd) < 3 {
		return resp.MakeErrorData("error: commands is invalid")
	}

	// 进行类型检查，会自动检查过期选项
	r, s := CheckOldType(db, string(cmd[1]), SET)

	if s == MISMATCH {
		return r

	} else if s == EMPTY {

		set := structure.NewSet()
		set.Add(string(cmd[2]))
		db.SetKey(string(cmd[1]), set)

	} else {

		set, _ := db.GetKey(string(cmd[1]))
		set.(*structure.Set).Add(string(cmd[2]))

	}

	// 重置 TTL
	db.RemoveTTL(string(cmd[1]))

	return resp.MakeStringData("OK")

}

func SIsMember(db *db.DataBase, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "sismember" {
		return resp.MakeErrorData("Server error")
	}

	if len(cmd) < 3 {
		return resp.MakeErrorData("error: commands is invalid")
	}

	// 进行类型检查，会自动检查过期选项
	r, s := CheckOldType(db, string(cmd[1]), SET)

	if s == MISMATCH {
		return r

	} else if s == EMPTY {

		return resp.MakeIntData(0)

	} else {

		set, _ := db.GetKey(string(cmd[1]))
		exist := set.(*structure.Set).Exist(string(cmd[2]))
		if !exist {
			return resp.MakeIntData(0)
		}
	}

	return resp.MakeIntData(1)
}

func RegisterSetCommand() {
	RegisterCommand("sadd", SADD)
	RegisterCommand("sismember", SIsMember)
}
