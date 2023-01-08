package cmd

import (
	"fmt"
	"github.com/tangrc99/MemTable/db/structure"
	"github.com/tangrc99/MemTable/resp"
	"strings"
)

type ValueType int

const (
	STRING ValueType = iota
	HASH
	SET
	ZSET
	LIST
)

func CheckType(value any, vt ValueType) resp.RedisData {

	// check if the value is string
	var typeOk bool

	if value != nil {
		// 如果已经存在，进行类型检查
		switch vt {
		case STRING:
			_, typeOk = value.([]byte)

		case HASH:
			// 复杂数据类型全部为指针
			_, typeOk = value.(*structure.Dict)

		case LIST:
			// 复杂数据类型全部为指针
			_, typeOk = value.(*structure.List)

		case SET:
			// 复杂数据类型全部为指针
			_, typeOk = value.(*structure.Set)

		case ZSET:
			// 复杂数据类型全部为指针
			_, typeOk = value.(*structure.ZSet)
		}

		if !typeOk {
			return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	return nil
}

func CheckCommandAndLength(cmd *[][]byte, name string, minLength int) (resp.RedisData, bool) {

	if len(*cmd) == 0 {
		return resp.MakeErrorData("ERR empty command"), false
	}

	cmdName := strings.ToLower(string((*cmd)[0]))
	if cmdName != name {
		return resp.MakeErrorData("Server error"), false
	}

	if len(*cmd) < minLength {
		return resp.MakeErrorData(fmt.Sprintf("ERR wrong number of arguments for '%s' command", (*cmd)[0])), false
	}

	return nil, true
}
