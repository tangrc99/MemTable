package db

import (
	"MemTable/db/structure"
)

type DataBase struct {
	dict    structure.Dict // 存储键值对
	ttlKeys structure.Dict // 存储过期键
}

// NewDataBase Create a new database impl
func NewDataBase() *DataBase {
	return &DataBase{
		dict:    structure.Dict{},
		ttlKeys: structure.Dict{},
	}
}

func (db_ *DataBase) CheckAndRemoveIfExpired(key string) bool {
	return false
}
