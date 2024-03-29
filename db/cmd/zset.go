package cmd

import (
	"fmt"
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/db/structure"
	"github.com/tangrc99/MemTable/resp"
	"strconv"
)

type String = structure.String

func zADD(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "zadd", 4)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))

	l := len(cmd)
	if l%2 == 1 {
		return resp.MakeErrorData("ERR wrong number of arguments for 'zadd' command")
	}

	if !ok {
		zset := structure.NewZSet()

		added := 0

		for i := 2; i < l; i += 2 {

			score, err := strconv.ParseFloat(string(cmd[i]), 32)
			if err != nil {
				return resp.MakeErrorData("ERR value is not a valid float")
			}

			if zset.AddIfNotExist(structure.Float32(score), string(cmd[i+1])) {
				added++
			}
		}

		db.SetKey(string(cmd[1]), zset)
		db.ReviseNotify(string(cmd[1]), 0, zset.Cost())

		return resp.MakeIntData(int64(added))
	}

	// 进行类型检查，会自动检查过期选项
	if err := checkType(value, ZSET); err != nil {
		return err
	}

	scores := make([]structure.Float32, l/2-1)
	members := make([][]byte, l/2-1)

	for i := 2; i < l; i += 2 {

		score, err := strconv.ParseFloat(string(cmd[i]), 32)
		if err != nil {
			return resp.MakeErrorData("ERR value is not a valid float")
		}
		scores[i/2-1] = structure.Float32(score)
		members[i/2-1] = cmd[i+1]
	}

	zsetVal := value.(*structure.ZSet)

	oldCost := zsetVal.Cost()

	added := 0

	for i, score := range scores {

		if zsetVal.AddIfNotExist(score, string(members[i])) {
			added++
		}
	}

	// 重置 TTL
	db.RemoveTTL(string(cmd[1]))
	db.ReviseNotify(string(cmd[1]), oldCost, zsetVal.Cost())

	return resp.MakeIntData(int64(added))
}

func zCount(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "zcount", 4)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(0)
	}

	// 进行类型检查，会自动检查过期选项
	if err := checkType(value, ZSET); err != nil {
		return err
	}

	zsetVal := value.(*structure.ZSet)

	min, err := strconv.ParseFloat(string(cmd[2]), 32)
	if err != nil {
		return resp.MakeErrorData("ERR value is not a valid float")
	}
	max, err := strconv.ParseFloat(string(cmd[3]), 32)
	if err != nil {
		return resp.MakeErrorData("ERR value is not a valid float")
	}

	count := zsetVal.CountByRange(structure.Float32(min), structure.Float32(max))
	return resp.MakeIntData(int64(count))
}

func zCard(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "zcard", 2)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(0)
	}

	// 进行类型检查，会自动检查过期选项
	if err := checkType(value, ZSET); err != nil {
		return err
	}

	zsetVal := value.(*structure.ZSet)

	count := zsetVal.Size()

	return resp.MakeIntData(int64(count))
}

func zRem(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "zrem", 3)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(0)
	}

	// 进行类型检查，会自动检查过期选项
	if err := checkType(value, ZSET); err != nil {
		return err
	}

	zsetVal := value.(*structure.ZSet)
	oldCost := zsetVal.Cost()
	deleted := 0

	for _, key := range cmd[2:] {
		if zsetVal.Delete(string(key)) {
			deleted++
		}
	}
	db.ReviseNotify(string(cmd[1]), oldCost, zsetVal.Cost())
	return resp.MakeIntData(int64(deleted))
}

func zIncrBy(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "zincrby", 4)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {

		zset := structure.NewZSet()

		increment, err := strconv.ParseFloat(string(cmd[2]), 32)
		if err != nil {
			return resp.MakeErrorData("ERR value is not a valid float")
		}

		zset.Add(structure.Float32(increment), string(cmd[3]))

		db.SetKey(string(cmd[1]), zset)

		return resp.MakeBulkData(cmd[2])
	}

	// 进行类型检查，会自动检查过期选项
	if err := checkType(value, ZSET); err != nil {
		return err
	}

	zsetVal := value.(*structure.ZSet)

	increment, err := strconv.ParseFloat(string(cmd[2]), 32)
	if err != nil {
		return resp.MakeErrorData("ERR value is not a valid float")
	}

	score, ok := zsetVal.IncrScore(string(cmd[3]), structure.Float32(increment))
	if !ok {

		zsetVal.Add(structure.Float32(increment), string(cmd[3]))
		return resp.MakeBulkData(cmd[2])
	}

	db.ReviseNotify(string(cmd[1]), 0, 0)

	return resp.MakeBulkData([]byte(fmt.Sprintf("%f", score)))
}

//func zLEXCount(db *db.DataBase, cmd [][]byte) resp.RedisData        {}

// zRange : zrange salary 0 -1 WITHSCORES
func zRange(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "zrange", 4)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeEmptyArrayData()
	}

	// 进行类型检查，会自动检查过期选项
	if err := checkType(value, ZSET); err != nil {
		return err
	}

	zsetVal := value.(*structure.ZSet)

	start, w := strconv.Atoi(string(cmd[2]))
	if w != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	end, w := strconv.Atoi(string(cmd[3]))
	if w != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	keys, n := zsetVal.Pos(start, end)
	res := make([]resp.RedisData, n)
	for i, key := range keys {
		res[i] = resp.MakeBulkData([]byte(key.(String)))
	}

	return resp.MakeArrayData(res)
}

func zRevRange(db *db.DataBase, cmd [][]byte) resp.RedisData { // 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "zrevrange", 4)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeEmptyArrayData()
	}

	// 进行类型检查，会自动检查过期选项
	if err := checkType(value, ZSET); err != nil {
		return err
	}

	zsetVal := value.(*structure.ZSet)

	start, w := strconv.Atoi(string(cmd[2]))
	if w != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	end, w := strconv.Atoi(string(cmd[3]))
	if w != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	keys, n := zsetVal.Pos(start, end)
	res := make([]resp.RedisData, n)
	for i, key := range keys {
		res[n-i-1] = resp.MakeBulkData([]byte(key.(String)))
	}

	return resp.MakeArrayData(res)
}

// zRank 显示 key 的 score 的排名，从小到大
func zRank(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "zrank", 3)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	// 进行类型检查，会自动检查过期选项
	if err := checkType(value, ZSET); err != nil {
		return err
	}

	zsetVal := value.(*structure.ZSet)

	score, ok := zsetVal.GetScoreByKey(string(cmd[2]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	rank := zsetVal.PosByScore(score)

	return resp.MakeIntData(int64(rank))
}

// zRank 显示 key 的 score 的排名，从大到小
func zRevRank(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "zrevrank", 3)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	// 进行类型检查，会自动检查过期选项
	if err := checkType(value, ZSET); err != nil {
		return err
	}

	zsetVal := value.(*structure.ZSet)

	score, ok := zsetVal.GetScoreByKey(string(cmd[2]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	rank := zsetVal.Size() - zsetVal.PosByScore(score) - 1

	return resp.MakeIntData(int64(rank))
}

func zScore(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "zscore", 3)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeStringData("nil")
	}

	// 进行类型检查，会自动检查过期选项
	if err := checkType(value, ZSET); err != nil {
		return err
	}

	zsetVal := value.(*structure.ZSet)

	score, ok := zsetVal.GetScoreByKey(string(cmd[2]))
	if !ok {
		return resp.MakeStringData("nil")
	}
	return resp.MakeStringData(fmt.Sprintf("%f", score))
}

func zRemRangeByRank(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "zremrangebyrank", 4)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(0)
	}

	// 进行类型检查，会自动检查过期选项
	if err := checkType(value, ZSET); err != nil {
		return err
	}

	zsetVal := value.(*structure.ZSet)

	start, w := strconv.Atoi(string(cmd[2]))
	if w != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	end, w := strconv.Atoi(string(cmd[3]))
	if w != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	oldCost := zsetVal.Cost()

	deleted := zsetVal.DeleteRange(start, end)

	db.ReviseNotify(string(cmd[1]), oldCost, zsetVal.Cost())

	return resp.MakeIntData(int64(deleted))
}

func zRemRangeByScore(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "zremrangebyscore", 4)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(0)
	}

	// 进行类型检查，会自动检查过期选项
	if err := checkType(value, ZSET); err != nil {
		return err
	}

	zsetVal := value.(*structure.ZSet)

	min, err := strconv.ParseFloat(string(cmd[2]), 32)
	if err != nil {
		return resp.MakeErrorData("ERR value is not a valid float")
	}
	max, err := strconv.ParseFloat(string(cmd[3]), 32)
	if err != nil {
		return resp.MakeErrorData("ERR value is not a valid float")
	}

	oldCost := zsetVal.Cost()

	deleted := zsetVal.DeleteRangeByScore(structure.Float32(min), structure.Float32(max))

	db.ReviseNotify(string(cmd[1]), oldCost, zsetVal.Cost())

	return resp.MakeIntData(int64(deleted))
}

func zRangeByScore(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "zrangebyscore", 4)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeEmptyArrayData()
	}

	// 进行类型检查，会自动检查过期选项
	if err := checkType(value, ZSET); err != nil {
		return err
	}

	zsetVal := value.(*structure.ZSet)

	min, err := strconv.ParseFloat(string(cmd[2]), 32)
	if err != nil {
		return resp.MakeErrorData("ERR value is not a valid float")
	}
	max, err := strconv.ParseFloat(string(cmd[3]), 32)
	if err != nil {
		return resp.MakeErrorData("ERR value is not a valid float")
	}

	keys, n := zsetVal.GetKeysByRange(structure.Float32(min), structure.Float32(max))

	res := make([]resp.RedisData, n)
	for i, key := range keys {
		res[i] = resp.MakeBulkData([]byte(key))
	}

	return resp.MakeArrayData(res)
}

func zRevRangeByScore(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "zrevrangebyscore", 4)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeEmptyArrayData()
	}

	// 进行类型检查，会自动检查过期选项
	if err := checkType(value, ZSET); err != nil {
		return err
	}

	zsetVal := value.(*structure.ZSet)

	min, err := strconv.ParseFloat(string(cmd[2]), 32)
	if err != nil {
		return resp.MakeErrorData("ERR value is not a valid float")
	}
	max, err := strconv.ParseFloat(string(cmd[3]), 32)
	if err != nil {
		return resp.MakeErrorData("ERR value is not a valid float")
	}

	keys, n := zsetVal.GetKeysByRange(structure.Float32(min), structure.Float32(max))

	res := make([]resp.RedisData, n)
	for i, key := range keys {
		res[n-i-1] = resp.MakeBulkData([]byte(key))
	}

	return resp.MakeArrayData(res)
}

//func zRemRangeByLEX(db *db.DataBase, cmd [][]byte) resp.RedisData   {}
//func zRevRangeByLEX(db *db.DataBase, cmd [][]byte) resp.RedisData   {}
//func zUnion(db *db.DataBase, cmd [][]byte) resp.RedisData             {}
//func zUnionStore(db *db.DataBase, cmd [][]byte) resp.RedisData             {}

func registerZSetCommands() {
	registerCommand("zadd", zADD, WR)
	registerCommand("zcount", zCount, RD)
	registerCommand("zcard", zCard, RD)
	registerCommand("zrem", zRem, WR)
	registerCommand("zincrby", zIncrBy, WR)
	registerCommand("zscore", zScore, RD)
	registerCommand("zrank", zRank, RD)
	registerCommand("zrevrank", zRevRank, RD)
	registerCommand("zremrangebyscore", zRemRangeByScore, WR)
	registerCommand("zremrangebyrank", zRemRangeByRank, WR)
	registerCommand("zrange", zRange, RD)
	registerCommand("zrevrange", zRevRange, RD)
	registerCommand("zrangebyscore", zRangeByScore, RD)
	registerCommand("zrevrangebyscore", zRevRangeByScore, RD)

}
