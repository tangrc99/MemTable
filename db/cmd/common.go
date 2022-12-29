package cmd

import (
	"MemTable/db"
	"MemTable/db/structure"
	"MemTable/resp"
)

type ValueType int

const (
	STRING ValueType = iota
	HASH
	SET
	ZSET
	LIST
)

type ValueStatus int

const (
	EMPTY ValueStatus = iota
	MATCH
	MISMATCH
)

func CheckOldType(db *db.DataBase, key string, vt ValueType) (resp.RedisData, ValueStatus) {

	oldVal, oldOk := db.GetKey(key)
	// check if the value is string
	var typeOk bool

	// 如果已经存在，进行类型检查
	if oldOk {

		switch vt {
		case STRING:
			_, typeOk = oldVal.(string)

		case HASH:
			// 复杂数据类型全部为指针
			_, typeOk = oldVal.(*structure.Dict)

		case LIST:
			// 复杂数据类型全部为指针
			_, typeOk = oldVal.(*structure.List)

		case SET:
			// 复杂数据类型全部为指针
			_, typeOk = oldVal.(*structure.Set)

		case ZSET:
			// 复杂数据类型全部为指针
			_, typeOk = oldVal.(*structure.ZSet)
		}

		if !typeOk {
			return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value"), MISMATCH
		} else {
			return nil, MATCH
		}
	}
	return nil, EMPTY
}
