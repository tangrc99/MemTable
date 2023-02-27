package cmd

import (
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/resp"
)

func bfAdd(base *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "bf.add", 3)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	//value, ok := base.GetKey(string(cmd[1]))
	//if !ok {
	//	structure.NewBloomFilter(1000,0.01)
	//}

	return resp.MakeIntData(1)
}
func bfMAdd(base *db.DataBase, cmd [][]byte) resp.RedisData {
	return resp.MakeIntData(1)

}
func bfExists(base *db.DataBase, cmd [][]byte) resp.RedisData {
	return resp.MakeIntData(1)

}
func bfMExists(base *db.DataBase, cmd [][]byte) resp.RedisData {
	return resp.MakeIntData(1)
}
func bfInfo(base *db.DataBase, cmd [][]byte) resp.RedisData {
	//1) Capacity
	//2) (integer) 100
	//3) Size
	//4) (integer) 240
	//5) Number of filters
	//6) (integer) 1
	//7) Number of items inserted
	//8) (integer) 1
	//9) Expansion rate
	//10) (integer) 2

	return resp.MakeArrayData(nil)
}
func bfReserve(base *db.DataBase, cmd [][]byte) resp.RedisData {
	return resp.MakeStringData("OK")
}

func RegisterBloomFilterCommands() {

	registerCommand("bf.add", bfAdd, WR)
	registerCommand("bf.madd", bfMAdd, WR)
	registerCommand("bf.exists", bfExists, RD)
	registerCommand("bf.mexists", bfMExists, RD)
	registerCommand("bf.info", bfInfo, RD)
	registerCommand("bf.reserve", bfReserve, WR)

}
