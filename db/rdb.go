package db

import (
	"errors"
	"fmt"
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"
	"github.com/tangrc99/MemTable/db/eviction"
	"github.com/tangrc99/MemTable/db/structure"
)

// Encode 将阻塞地将 DataBase 中的全部键值对写入到 rdb 文件中，如果写入过程发生错误将返回 error
func (db_ *DataBase) Encode(enc *core.Encoder) error {

	var err error = nil

	dicts, _ := db_.dict.GetAll()

	keys := 0
	ttls := 0

	for _, dict := range *dicts {
		for k, v := range dict {

			v = v.(*eviction.Item).Value

			keys++
			var ttl uint64 = 0

			if expiredAt, ok := db_.ttlKeys.Get(k); ok {

				ttl = uint64(expiredAt.(Int64) * 1000)
				ttls++
			}

			if str, ok := v.(structure.Slice); ok {

				if ttl > 0 {
					err = enc.WriteStringObject(k, str, encoder.WithTTL(ttl))
				} else {
					err = enc.WriteStringObject(k, str)
				}

			} else if list, ok := v.(*structure.List); ok {

				values, n := list.Range(0, -1)
				listVal := make([][]byte, n)
				for i, value := range values {
					listVal[i] = value.(structure.Slice)
				}
				if ttl > 0 {
					err = enc.WriteListObject(k, listVal, encoder.WithTTL(ttl))
				} else {
					err = enc.WriteListObject(k, listVal)
				}

			} else if set, ok := v.(*structure.Set); ok {

				members, _ := set.KeysByte("")
				if ttl > 0 {
					err = enc.WriteSetObject(k, members, encoder.WithTTL(ttl))
				} else {
					err = enc.WriteSetObject(k, members)
				}

			} else if zset, ok := v.(*structure.ZSet); ok {

				members, n := zset.Pos(0, -1)
				entrys := make([]*model.ZSetEntry, n)
				for i, member := range members {
					score, _ := zset.GetScoreByKey(string(member.(structure.String)))
					entrys[i] = &model.ZSetEntry{
						Score:  float64(score),
						Member: string(member.(structure.String)),
					}
				}
				if ttl > 0 {
					err = enc.WriteZSetObject(k, entrys, encoder.WithTTL(ttl))
				} else {
					err = enc.WriteZSetObject(k, entrys)
				}

			} else if hash, ok := v.(*structure.Dict); ok {

				kvs, _ := hash.GetAll()
				entrys := make(map[string][]byte)
				for key, value := range (*kvs)[0] {
					entrys[key] = value.(structure.Slice)
				}

				if ttl > 0 {
					err = enc.WriteHashMapObject(k, entrys, encoder.WithTTL(ttl))
				} else {
					err = enc.WriteHashMapObject(k, entrys)
				}
			} else {

				panic(fmt.Sprintf("Unexpected type %T", v))

			}

			if err != nil {
				return err
			}
		}
	}

	if ttls != db_.TTLSize() {
		return errors.New(fmt.Sprintf("DB TTL Size Not Matched, Expected %d But %d", db_.TTLSize(), ttls))
	}
	if keys != db_.Size() {
		return errors.New(fmt.Sprintf("DB Size Not Matched, Expected %d But %d", db_.Size(), keys))
	}
	return err
}
