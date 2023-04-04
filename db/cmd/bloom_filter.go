package cmd

import (
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/db/structure"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/utils"
	"strconv"
)

func bfAdd(base *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "bf.add", 3)
	if !ok {
		return e
	}

	//get 会自动检查是否过期
	value, ok := base.GetKey(string(cmd[1]))
	var bloom *structure.Bloom
	if !ok {
		bloom = structure.NewBloomFilter(1000, 0.01)
		base.SetKey(string(cmd[1]), bloom)
	} else {

		bloom, ok = value.(*structure.Bloom)
		if !ok {
			return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	ret := 0

	if bloom.AddIfNotHas(utils.MemHash(cmd[2])) {
		ret++
	}

	return resp.MakeIntData(int64(ret))
}

func bfMAdd(base *db.DataBase, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "bf.madd", 3)
	if !ok {
		return e
	}

	//get 会自动检查是否过期
	value, ok := base.GetKey(string(cmd[1]))
	var bloom *structure.Bloom
	if !ok {
		bloom = structure.NewBloomFilter(1000, 0.01)
		base.SetKey(string(cmd[1]), bloom)
	} else {

		bloom, ok = value.(*structure.Bloom)
		if !ok {
			return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	ret := 0
	for _, ele := range cmd[2:] {
		if bloom.AddIfNotHas(utils.MemHash(ele)) {
			ret++
		}
	}

	return resp.MakeIntData(int64(ret))

}

func bfExists(base *db.DataBase, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "bf.exists", 3)
	if !ok {
		return e
	}

	//get 会自动检查是否过期
	value, ok := base.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(0)
	}

	bloom, ok := value.(*structure.Bloom)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if exist := bloom.Has(utils.MemHash(cmd[2])); !exist {
		return resp.MakeIntData(0)
	}

	return resp.MakeIntData(1)

}

func bfMExists(base *db.DataBase, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "bf.mexists", 3)
	if !ok {
		return e
	}

	//get 会自动检查是否过期
	value, ok := base.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(0)
	}

	bloom, ok := value.(*structure.Bloom)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	ret := int64(0)

	for _, ele := range cmd[2:] {
		if exist := bloom.Has(utils.MemHash(ele)); exist {
			ret++
		}
	}

	return resp.MakeIntData(ret)
}

func bfInfo(base *db.DataBase, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "bf.info", 2)
	if !ok {
		return e
	}

	//get 会自动检查是否过期
	value, ok := base.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(0)
	}

	bloom, ok := value.(*structure.Bloom)
	if !ok {
		return resp.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	ret := make([]resp.RedisData, 0, 10)

	capacity := bloom.Capacity()
	size := bloom.Cost()
	filters := bloom.FilterNum()
	items := bloom.Items()

	ret = append(ret, resp.MakeBulkData([]byte("Capacity")))
	ret = append(ret, resp.MakeIntData(int64(capacity)))

	ret = append(ret, resp.MakeBulkData([]byte("Size")))
	ret = append(ret, resp.MakeIntData(int64(size)))

	ret = append(ret, resp.MakeBulkData([]byte("Number of filters")))
	ret = append(ret, resp.MakeIntData(filters))

	ret = append(ret, resp.MakeBulkData([]byte("Number of items inserted")))
	ret = append(ret, resp.MakeIntData(int64(items)))

	return resp.MakeArrayData(ret)
}

func bfReserve(base *db.DataBase, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "bf.reserve", 4)
	if !ok {
		return e
	}

	//get 会自动检查是否过期
	if _, ok := base.GetKey(string(cmd[1])); ok {
		return resp.MakeErrorData("ERR item exists")
	}

	error_rate, err := strconv.ParseFloat(string(cmd[2]), 32)
	if err != nil {
		return resp.MakeErrorData("ERR error_rate is out of range")
	}

	capacity, err := strconv.ParseFloat(string(cmd[3]), 32)
	if err != nil {
		return resp.MakeErrorData("ERR capacity is out of range")
	}

	base.SetKey(string(cmd[1]), structure.NewBloomFilter(capacity, error_rate))

	return resp.MakeStringData("OK")
}

func registerBloomFilterCommands() {

	registerCommand("bf.add", bfAdd, WR)
	registerCommand("bf.madd", bfMAdd, WR)
	registerCommand("bf.exists", bfExists, RD)
	registerCommand("bf.mexists", bfMExists, RD)
	registerCommand("bf.info", bfInfo, RD)
	registerCommand("bf.reserve", bfReserve, WR)

}
