package cmd

import (
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/db/structure"
	"github.com/tangrc99/MemTable/resp"
	"strconv"
)

func sadd(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "sadd", 3)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))

	if !ok {
		set := structure.NewSet()

		added := 0
		for _, key := range cmd[2:] {
			if set.Add(string(key)) {
				added++
			}
		}

		db.SetKey(string(cmd[1]), set)
		return resp.MakeIntData(int64(added))
	}

	// 进行类型检查，会自动检查过期选项
	if err := checkType(value, SET); err != nil {
		return err
	}

	added := 0
	for _, key := range cmd[2:] {
		if value.(*structure.Set).Add(string(key)) {
			added++
		}
	}

	// 重置 TTL
	db.RemoveTTL(string(cmd[1]))

	return resp.MakeIntData(int64(added))
}

func sRem(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "srem", 3)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(0)
	}

	if err := checkType(value, SET); err != nil {
		return err
	}

	set := value.(*structure.Set)
	deleted := 0

	for _, key := range cmd[2:] {
		ok := set.Delete(string(key))
		if ok {
			deleted++
		}
	}
	return resp.MakeIntData(int64(deleted))
}

func scard(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "scard", 2)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(0)
	}

	if err := checkType(value, SET); err != nil {
		return err
	}

	return resp.MakeIntData(int64(value.(*structure.Set).Size()))
}

func sismember(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "sismember", 2)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))

	if !ok {
		return resp.MakeIntData(0)
	}

	// 进行类型检查，会自动检查过期选项
	if err := checkType(value, SET); err != nil {
		return err
	}

	set, _ := db.GetKey(string(cmd[1]))
	exist := set.(*structure.Set).Exist(string(cmd[2]))
	if !exist {
		return resp.MakeIntData(0)
	}

	return resp.MakeIntData(1)
}

func sMembers(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "smembers", 2)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeArrayData(nil)
	}

	if err := checkType(value, SET); err != nil {
		return err
	}

	setVal := value.(*structure.Set)

	res := make([]resp.RedisData, setVal.Size())

	ks, _ := setVal.KeysByte("")

	//sort.Strings(ks)

	for i, key := range ks {
		res[i] = resp.MakeBulkData(key)
	}
	return resp.MakeArrayData(res)
}

func sPop(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "spop", 3)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeArrayData(nil)
	}

	if err := checkType(value, SET); err != nil {
		return err
	}

	setVal := value.(*structure.Set)

	num, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	ks := setVal.RandomPop(num)

	res := make([]resp.RedisData, len(ks))

	i := 0
	for k := range ks {
		res[i] = resp.MakeBulkData([]byte(k))
		i++
	}
	return resp.MakeArrayData(res)
}

func sRandMember(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "spop", 3)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(0)
	}

	if err := checkType(value, SET); err != nil {
		return err
	}

	setVal := value.(*structure.Set)

	num, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	ks := setVal.RandomGet(num)

	res := make([]resp.RedisData, len(ks))

	i := 0
	for k := range ks {
		res[i] = resp.MakeBulkData([]byte(k))
		i++
	}
	return resp.MakeArrayData(res)
}

func sMove(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "smove", 4)
	if !ok {
		return e
	}

	// get 会自动检查是否过期
	value1, ok := db.GetKey(string(cmd[1]))
	if !ok {
		return resp.MakeIntData(0)
	}

	if err := checkType(value1, SET); err != nil {
		return err
	}

	setVal1 := value1.(*structure.Set)

	// get 会自动检查是否过期
	value2, ok := db.GetKey(string(cmd[2]))
	if !ok {
		return resp.MakeIntData(0)
	}

	if err := checkType(value2, SET); err != nil {
		return err
	}

	setVal2 := value2.(*structure.Set)

	if setVal1.Delete(string(cmd[3])) {
		setVal2.Add(string(cmd[3]))
		return resp.MakeIntData(1)
	}
	return resp.MakeIntData(0)
}

// sDiff 返回第一个集合中特有元素
func sDiff(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "sdiff", 2)
	if !ok {
		return e
	}

	sl := len(cmd) - 1
	sets := make([]*structure.Set, sl)

	for i, s := range cmd[1:] {
		value, ok := db.GetKey(string(s))
		if !ok {
			sets[i] = nil
			continue
		}

		if err := checkType(value, SET); err != nil {
			sets[i] = nil
			continue
		}

		sets[i] = value.(*structure.Set)
	}

	// 检查元素是否正确
	if sets[0] == nil {
		return resp.MakeArrayData(nil)
	}

	ks, n := sets[0].Keys("")

	res := make([]resp.RedisData, 0)

	for i := 0; i < n; i++ {
		ok := true
		for j := 1; j < sl && ok; j++ {

			if sets[j] == nil {
				continue
			}

			ok = !sets[j].Exist(ks[i])
		}
		if ok {
			res = append(res, resp.MakeBulkData([]byte(ks[i])))
		}
	}

	return resp.MakeArrayData(res)
}

func sDiffStore(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "sdiffstore", 3)
	if !ok {
		return e
	}

	sl := len(cmd) - 1
	sets := make([]*structure.Set, sl)

	for i, s := range cmd[2:] {
		value, ok := db.GetKey(string(s))
		if !ok {
			sets[i] = nil
			continue
		}

		if err := checkType(value, SET); err != nil {
			sets[i] = nil
			continue
		}

		sets[i] = value.(*structure.Set)
	}

	// 检查元素是否正确
	if sets[0] == nil {
		return resp.MakeIntData(0)
	}

	ks, n := sets[0].Keys("")

	dstSet := structure.NewSet()

	for i := 0; i < n; i++ {
		ok := true
		for j := 1; j < sl && ok; j++ {

			if sets[j] == nil {
				continue
			}

			ok = !sets[j].Exist(ks[i])
		}
		if ok {
			dstSet.Add(ks[i])
		}
	}

	db.SetKey(string(cmd[1]), dstSet)
	db.RemoveTTL(string(cmd[1]))
	return resp.MakeIntData(int64(dstSet.Size()))
}

// sInter 返回所有集合的交集，使用哈希表记录每个元素出现的次数，只有次数等于集合总数的元素才为交集
func sInter(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "sinter", 2)
	if !ok {
		return e
	}

	// 当集合只有一个情况下进行优化
	if len(cmd) == 2 {
		cmd[0] = []byte("smembers")
		return sMembers(db, cmd)
	}

	// 哈希表
	ks := make(map[string]int)

	sl := len(cmd) - 1

	res := make([]resp.RedisData, 0)

	// 取出所有集合元素指针，如果 key 不是集合，则存储为 nil
	for _, s := range cmd[1:] {
		value, ok := db.GetKey(string(s))
		if !ok {
			continue
		}

		if err := checkType(value, SET); err != nil {
			continue
		}

		s := value.(*structure.Set)
		// 取出集合的所有元素，并放入哈希表中
		sks, _ := s.Keys("")
		for _, k := range sks {

			if ks[k]++; ks[k] == sl {
				// 所有集合都有该元素
				res = append(res, resp.MakeBulkData([]byte(k)))
			}
		}
	}

	return resp.MakeArrayData(res)
}

func sInterStore(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "sinterstore", 3)
	if !ok {
		return e
	}

	dstSet := structure.NewSet()

	if len(cmd) == 3 {
		value, ok := db.GetKey(string(cmd[3]))
		if !ok {
			return resp.MakeIntData(0)
		}

		if err := checkType(value, SET); err != nil {
			return resp.MakeIntData(0)
		}

		ks, n := value.(*structure.Set).Keys("")
		for _, k := range ks {
			dstSet.Add(k)
		}
		db.SetKey(string(cmd[1]), dstSet)
		db.RemoveTTL(string(cmd[1]))
		return resp.MakeIntData(int64(n))
	}

	// 哈希表
	ks := make(map[string]int)
	sl := len(cmd) - 1

	// 取出所有集合元素指针，如果 key 不是集合，则存储为 nil
	for _, s := range cmd[1:] {
		value, ok := db.GetKey(string(s))
		if !ok {
			continue
		}

		if err := checkType(value, SET); err != nil {
			continue
		}

		s := value.(*structure.Set)
		// 取出集合的所有元素，并放入哈希表中
		sks, _ := s.Keys("")
		for _, k := range sks {

			if ks[k]++; ks[k] == sl {
				// 所有集合都有该元素
				dstSet.Add(k)
			}
		}
	}

	if dstSet.Size() == 0 {
		return resp.MakeIntData(0)
	}

	db.SetKey(string(cmd[1]), dstSet)
	db.RemoveTTL(string(cmd[1]))
	return resp.MakeIntData(int64(dstSet.Size()))
}

// sInter 返回所有集合的并集
func sUnion(db *db.DataBase, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "sunion", 2)
	if !ok {
		return e
	}

	sl := len(cmd) - 1
	sets := make([]*structure.Set, sl)
	counts := 0

	// 计算集合的总长度，进行内存分配
	for i, s := range cmd[1:] {
		value, ok := db.GetKey(string(s))
		if !ok {
			continue
		}

		if err := checkType(value, SET); err != nil {
			continue
		}

		sets[i] = value.(*structure.Set)
		counts += sets[i].Size()
	}

	// 去重
	ks := make(map[string]struct{})

	// 取出每个集合的元素
	for i := 0; i < sl; i++ {

		if sets[i] == nil {
			continue
		}

		sks, _ := sets[i].Keys("")

		for _, k := range sks {
			ks[k] = struct{}{}
		}

	}

	res := make([]resp.RedisData, len(ks))
	pos := 0
	for k := range ks {
		res[pos] = resp.MakeBulkData([]byte(k))
		pos++
	}

	return resp.MakeArrayData(res)
}

func sUnionStore(db *db.DataBase, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	e, ok := checkCommandAndLength(&cmd, "sunionstore", 3)
	if !ok {
		return e
	}

	dstSet := structure.NewSet()

	// 计算集合的总长度，进行内存分配
	for _, s := range cmd[1:] {
		value, ok := db.GetKey(string(s))
		if !ok {
			continue
		}

		if err := checkType(value, SET); err != nil {
			continue
		}

		s := value.(*structure.Set)
		ks, _ := s.Keys("")
		for _, k := range ks {
			dstSet.Add(k)
		}
	}

	if dstSet.Size() == 0 {
		return resp.MakeIntData(0)
	}

	db.SetKey(string(cmd[1]), dstSet)
	db.RemoveTTL(string(cmd[1]))
	return resp.MakeIntData(int64(dstSet.Size()))
}

/*


func sScan(db *db.DataBase, cmd [][]byte) resp.RedisData {}

*/

func registerSetCommands() {
	registerCommand("sadd", sadd, WR)
	registerCommand("scard", scard, RD)
	registerCommand("sismember", sismember, RD)
	registerCommand("srem", sRem, WR)
	registerCommand("smembers", sMembers, RD)
	registerCommand("spop", sPop, RD)
	registerCommand("srandmember", sRandMember, RD)
	registerCommand("smove", sMove, WR)

	registerCommand("sdiff", sDiff, RD)
	registerCommand("sdiffstore", sDiffStore, WR)
	registerCommand("sinter", sInter, RD)
	registerCommand("sinterstore", sInterStore, WR)
	registerCommand("sunion", sUnion, RD)
	registerCommand("sunionstore", sUnionStore, WR)
}
