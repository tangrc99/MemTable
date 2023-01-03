package cmd

import (
	"MemTable/db"
	"MemTable/db/structure"
	"MemTable/resp"
	"strconv"
)

func setbit(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "setbit", 4)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	var byteVal []byte

	if !ok {
		byteVal = make([]byte, 0)
	} else {
		// 进行类型检查，会自动检查过期选项
		if err := CheckType(value, STRING); err != nil {
			return err
		}
		byteVal = value.([]byte)
	}

	pos, err := strconv.Atoi(string(cmd[3]))
	if err != nil {
		return resp.MakeErrorData("ERR bit offset is not an integer or out of range")
	}

	bitVal, err := strconv.Atoi(string(cmd[3]))
	if err != nil {
		return resp.MakeErrorData("ERR bit is not an integer or out of range")
	}
	if bitVal != 0 && bitVal != 1 {
		return resp.MakeErrorData("ERR bit is not an integer or out of range")
	}

	bm := structure.NewBitMapFromBytes(byteVal)

	old := bm.GetSet(pos, byte(bitVal))
	db.SetKey(string(cmd[1]), ([]byte)(*bm))

	return resp.MakeIntData(int64(old))
}

func getbit(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "getbit", 3)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(0)
	}
	// 进行类型检查，会自动检查过期选项
	if err := CheckType(value, STRING); err != nil {
		return err
	}

	pos, err := strconv.Atoi(string(cmd[3]))
	if err != nil {
		return resp.MakeErrorData("ERR bit offset is not an integer or out of range")
	}

	bm := structure.NewBitMapFromBytes(value.([]byte))

	old := bm.Get(pos)

	return resp.MakeIntData(int64(old))
}

func bitcount(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "bitcount", 2)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(-1)
	}

	// 进行类型检查，会自动检查过期选项
	if err := CheckType(value, STRING); err != nil {
		return err
	}

	if len(cmd) == 3 {
		return resp.MakeErrorData("ERR syntax error")
	}

	start := 0
	end := -1

	if len(cmd) == 4 {

		s, err := strconv.Atoi(string(cmd[2]))
		if err != nil {
			return resp.MakeErrorData("ERR start is not an integer or out of range")
		}

		e, err := strconv.Atoi(string(cmd[3]))
		if err != nil {
			return resp.MakeErrorData("ERR end is not an integer or out of range")
		}
		start = s
		end = e
	}

	bm := structure.NewBitMapFromBytes(value.([]byte))

	count := bm.Count(start, end)

	return resp.MakeIntData(int64(count))
}

func bitpos(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "bitpos", 3)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(-1)
	}

	// 进行类型检查，会自动检查过期选项
	if err := CheckType(value, STRING); err != nil {
		return err
	}

	bitVal, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return resp.MakeErrorData("ERR bit offset is not an integer or out of range")
	}

	start := 0
	end := -1

	if len(cmd) >= 4 {
		s, err := strconv.Atoi(string(cmd[3]))
		if err != nil {
			return resp.MakeErrorData("ERR start is not an integer or out of range")
		}
		start = s

	}

	if len(cmd) == 5 {
		e, err := strconv.Atoi(string(cmd[4]))
		if err != nil {
			return resp.MakeErrorData("ERR end is not an integer or out of range")
		}
		end = e
	}

	bm := structure.NewBitMapFromBytes(value.([]byte))

	pos := bm.Pos(byte(bitVal), start, end)

	return resp.MakeIntData(int64(pos))
}

func RegisterBitMapCommands() {
	RegisterCommand("setbit", setbit, WR)
	RegisterCommand("getbit", getbit, RD)
	RegisterCommand("bitcount", bitcount, RD)
	RegisterCommand("bitpos", bitpos, RD)
}

/*
func bitfield(db *db.DataBase, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "bitfield", 3)
	if !ok {
		return e
	}

	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(-1)
	}

	// 进行类型检查，会自动检查过期选项
	if err := CheckType(value, STRING); err != nil {
		return err
	}

	if len(cmd) == 2 {
		return resp.MakeEmptyArrayData()
	}

	commands := make([][]any, 0)

	// 先解析命令
	cmdLen := len(cmd)
	for i := 2; i < cmdLen; {

		if strings.ToLower(string(cmd[i])) == "get" {

			commands = append(commands, make([]any, 4))
			commands[i-2][0] = "get"

			if cmdLen-i < 3 {
				return resp.MakeErrorData("ERR wrong number of arguments for 'bitfield' command")
			}

			commands[i-2][1] = string(cmd[i+1][0])
			if commands[i-2][1] != "u" && commands[i-2][1] != "i" {
				return resp.MakeErrorData("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
			}

			nums, err := strconv.Atoi(string(cmd[i+1][1:]))
			if err != nil || nums >= 64 || (commands[i-2][1] == "u" && nums == 64) {
				return resp.MakeErrorData("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
			}
			commands[i-2][2] = nums

			pos, err := strconv.Atoi(string(cmd[i+2]))
			if err != nil {
				return resp.MakeErrorData("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
			}

			commands[i-2][3] = pos
			i += 3

		} else if strings.ToLower(string(cmd[i])) == "set" {

			commands = append(commands, make([]any, 4))
			commands[i-2][0] = "set"

			if cmdLen-i < 4 {
				return resp.MakeErrorData("ERR wrong number of arguments for 'bitfield' command")
			}

			commands[i-2][1] = string(cmd[i+1][0])
			if commands[i-2][1] != "u" && commands[i-2][1] != "i" {
				return resp.MakeErrorData("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
			}

			nums, err := strconv.Atoi(string(cmd[i+1][1:]))
			if err != nil || nums >= 64 || (commands[i-2][1] == "u" && nums == 64) {
				return resp.MakeErrorData("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
			}
			commands[i-2][2] = nums

			pos, err := strconv.Atoi(string(cmd[i+2]))
			if err != nil {
				return resp.MakeErrorData("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
			}

			commands[i-2][3] = pos

			val, err := strconv.Atoi(string(cmd[i+3]))
			if err != nil || (commands[i-2][1] == "u" && 2^pos < val) ||
				(commands[i-2][1] == "i" && float64(2^(pos-1)) < amath.Abs(float64(val))) {
				return resp.MakeErrorData("ERR value is not an integer or out of range")
			}
			commands[i-2][3] = val

			i += 4

		} else if strings.ToLower(string(cmd[i])) == "incrby" {

			commands = append(commands, make([]any, 4))
			commands[i-2][0] = "set"

			if cmdLen-i < 4 {
				return resp.MakeErrorData("ERR wrong number of arguments for 'bitfield' command")
			}

			commands[i-2][1] = string(cmd[i+1][0])
			if commands[i-2][1] != "u" && commands[i-2][1] != "i" {
				return resp.MakeErrorData("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
			}

			nums, err := strconv.Atoi(string(cmd[i+1][1:]))
			if err != nil || nums >= 64 || (commands[i-2][1] == "u" && nums == 64) {
				return resp.MakeErrorData("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
			}
			commands[i-2][2] = nums

			pos, err := strconv.Atoi(string(cmd[i+2]))
			if err != nil {
				return resp.MakeErrorData("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
			}

			commands[i-2][3] = pos

			val, err := strconv.Atoi(string(cmd[i+3]))
			if err != nil || (commands[i-2][1] == "u" && 2^pos < val) ||
				(commands[i-2][1] == "i" && float64(2^(pos-1)) < amath.Abs(float64(val))) {
				return resp.MakeErrorData("ERR value is not an integer or out of range")
			}
			commands[i-2][3] = val

			i += 4

		} else {
			return resp.MakeErrorData("ERR syntax error")
		}

	}

	bm := structure.NewBitMapFromBytes(value.([]byte))

}*/
