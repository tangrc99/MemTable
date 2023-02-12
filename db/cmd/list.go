package cmd

import (
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/db/structure"
	"github.com/tangrc99/MemTable/resp"
	"strconv"
	"strings"
)

func lLen(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "llen", 2)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(0)
	}

	err := checkType(value, LIST)
	if err != nil {
		return err
	}

	l := value.(*structure.List).Size()

	return resp.MakeIntData(int64(l))
}

func lPush(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "lpush", 3)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		value = structure.NewList()
		db.SetKey(string(cmd[1]), value)
	}

	err := checkType(value, LIST)
	if err != nil {
		return err
	}

	listVal := value.(*structure.List)

	n := 0

	for _, ele := range cmd[2:] {
		n++
		listVal.PushFront(ele)
	}

	return resp.MakeIntData(int64(n))
}

func rPush(db *db.DataBase, cmd [][]byte) resp.RedisData { // 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "rpush", 3)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		value = structure.NewList()
		db.SetKey(string(cmd[1]), value)
	}

	err := checkType(value, LIST)
	if err != nil {
		return err
	}

	listVal := value.(*structure.List)

	n := 0

	for _, ele := range cmd[2:] {
		n++
		listVal.PushBack(ele)
	}

	return resp.MakeIntData(int64(n))
}

func lPop(db *db.DataBase, cmd [][]byte) resp.RedisData {
	e, ok := checkCommandAndLength(&cmd, "lpop", 2)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	e = checkType(value, LIST)
	if e != nil {
		return e
	}

	listVal := value.(*structure.List)

	count := 1

	if len(cmd) == 3 {
		var w error
		count, w = strconv.Atoi(string(cmd[2]))
		if w != nil {
			return resp.MakeErrorData("ERR value is not an integer or out of range")
		}
	}

	if count >= listVal.Size() {
		count = listVal.Size()
		// 全部取出元素，需要删除
		db.DeleteKey(string(cmd[1]))
	}

	res := make([]resp.RedisData, count)

	for i := 0; i < count; i++ {
		v, _ := listVal.PopFront().([]byte)
		res[i] = resp.MakeBulkData(v)
	}

	return resp.MakeArrayData(res)
}

func rPop(db *db.DataBase, cmd [][]byte) resp.RedisData {
	e, ok := checkCommandAndLength(&cmd, "rpop", 2)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	e = checkType(value, LIST)
	if e != nil {
		return e
	}

	listVal := value.(*structure.List)

	count := 1

	if len(cmd) == 3 {
		var w error
		count, w = strconv.Atoi(string(cmd[2]))
		if w != nil {
			return resp.MakeErrorData("ERR value is not an integer or out of range")
		}
	}

	if count >= listVal.Size() {
		count = listVal.Size()
		// 全部取出元素，需要删除
		db.DeleteKey(string(cmd[1]))
	}

	res := make([]resp.RedisData, count)

	for i := 0; i < count; i++ {
		v, _ := listVal.PopBack().([]byte)
		res[i] = resp.MakeBulkData(v)
	}

	return resp.MakeArrayData(res)
}

func lIndex(db *db.DataBase, cmd [][]byte) resp.RedisData {
	e, ok := checkCommandAndLength(&cmd, "lindex", 3)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	e = checkType(value, LIST)
	if e != nil {
		return e
	}

	listVal := value.(*structure.List)

	pos, w := strconv.Atoi(string(cmd[2]))
	if w != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	nodeVal, ok := listVal.Pos(pos)
	if !ok {
		return resp.MakeStringData("nil")
	}

	return resp.MakeBulkData(nodeVal.([]byte))
}

func lPos(db *db.DataBase, cmd [][]byte) resp.RedisData {
	e, ok := checkCommandAndLength(&cmd, "lpos", 3)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	e = checkType(value, LIST)
	if e != nil {
		return e
	}

	listVal := value.(*structure.List)

	pos := 0

	for cur := listVal.FrontNode(); cur != nil; cur = cur.Next() {
		byteVal := cur.Value.([]byte)

		if string(byteVal) == string(cmd[2]) {
			return resp.MakeIntData(int64(pos))
		}
		pos++
	}
	return resp.MakeStringData("nil")
}

func lSet(db *db.DataBase, cmd [][]byte) resp.RedisData {
	e, ok := checkCommandAndLength(&cmd, "lset", 4)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeErrorData("ERR no such key")
	}

	e = checkType(value, LIST)
	if e != nil {
		return e
	}

	listVal := value.(*structure.List)

	pos, w := strconv.Atoi(string(cmd[2]))
	if w != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	ok = listVal.Set(cmd[3], pos)
	if !ok {
		return resp.MakeErrorData("ERR index out of range")
	}

	return resp.MakeStringData("OK")
}

func lRem(db *db.DataBase, cmd [][]byte) resp.RedisData {

	e, ok := checkCommandAndLength(&cmd, "lrem", 4)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(0)
	}

	e = checkType(value, LIST)
	if e != nil {
		return e
	}

	listVal := value.(*structure.List)

	count, w := strconv.Atoi(string(cmd[2]))
	if w != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	deleted := 0
	for cur := listVal.FrontNode(); cur != nil && deleted <= count; {
		byteVal := cur.Value.([]byte)

		if string(byteVal) == string(cmd[2]) {

			nxt := cur.Next()
			listVal.RemoveNode(cur)
			cur = nxt
			deleted++
		} else {
			cur = cur.Next()
		}

	}
	return resp.MakeIntData(int64(deleted))
}

func lRange(db *db.DataBase, cmd [][]byte) resp.RedisData {
	e, ok := checkCommandAndLength(&cmd, "lrange", 4)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeArrayData(nil)
	}

	e = checkType(value, LIST)
	if e != nil {
		return e
	}

	listVal := value.(*structure.List)

	start, w := strconv.Atoi(string(cmd[2]))
	if w != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	end, w := strconv.Atoi(string(cmd[3]))
	if w != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	values, n := listVal.Range(start, end)
	if n == 0 {
		return resp.MakeArrayData(nil)
	}
	res := make([]resp.RedisData, len(values))
	for i, v := range values {
		res[i] = resp.MakeBulkData(v.([]byte))
	}

	return resp.MakeArrayData(res)
}

// lTrim 删除指定范围外的所有元素
func lTrim(db *db.DataBase, cmd [][]byte) resp.RedisData {
	e, ok := checkCommandAndLength(&cmd, "ltrim", 4)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeStringData("OK")
	}

	e = checkType(value, LIST)
	if e != nil {
		return e
	}

	listVal := value.(*structure.List)

	start, w := strconv.Atoi(string(cmd[2]))
	if w != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	end, w := strconv.Atoi(string(cmd[3]))
	if w != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	listVal.Trim(start, end)

	return resp.MakeStringData("OK")
}

func lMove(db *db.DataBase, cmd [][]byte) resp.RedisData {
	e, ok := checkCommandAndLength(&cmd, "lmove", 5)
	if !ok {
		return e
	}

	value1, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	e = checkType(value1, LIST)
	if e != nil {
		return e
	}

	listVal1 := value1.(*structure.List)

	value2, ok := db.GetKey(string(cmd[2]))
	if !ok {
		value2 = structure.NewList()
		db.SetKey(string(cmd[2]), value2)
	}

	e = checkType(value2, LIST)
	if e != nil {
		return e
	}

	listVal2 := value2.(*structure.List)

	// 这里目的为每一个 cmd 只比较一次
	if strings.ToUpper(string(cmd[3])) == "LEFT" {

		if strings.ToUpper(string(cmd[4])) == "LEFT" {

			listVal2.PushFront(listVal1.PopFront())

		} else if strings.ToUpper(string(cmd[4])) == "RIGHT" {

			listVal2.PushBack(listVal1.PopFront())

		} else {

			return resp.MakeErrorData("ERR syntax error")
		}

	} else if strings.ToUpper(string(cmd[3])) == "RIGHT" {

		if strings.ToUpper(string(cmd[4])) == "LEFT" {

			listVal2.PushFront(listVal1.PopBack())

		} else if strings.ToUpper(string(cmd[4])) == "RIGHT" {

			listVal2.PushBack(listVal1.PopBack())

		} else {

			return resp.MakeErrorData("ERR syntax error")
		}

	} else {
		return resp.MakeErrorData("ERR syntax error")
	}

	return resp.MakeStringData("OK")

}

/*


func bLPop(db *db.DataBase, cmd [][]byte) resp.RedisData  {}
func bRPop(db *db.DataBase, cmd [][]byte) resp.RedisData  {}
*/

func registerListCommands() {
	registerCommand("llen", lLen, RD)
	registerCommand("lpush", lPush, WR)
	registerCommand("lpop", lPop, WR)
	registerCommand("rpush", rPush, WR)
	registerCommand("rpop", rPop, WR)
	registerCommand("lindex", lIndex, RD)
	registerCommand("lpos", lPos, RD)
	registerCommand("lset", lSet, WR)
	registerCommand("lrem", lRem, WR)
	registerCommand("lrange", lRange, RD)
	registerCommand("ltrim", lTrim, WR)
	registerCommand("lmove", lMove, WR)
}
