package cmd

import (
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/global"
)

type ExecStatus = global.ExecStatus

const WR = global.WR
const RD = global.RD

type command = func(base *db.DataBase, cmd [][]byte) resp.RedisData

func registerCommand(name string, cmd command, status ExecStatus) {
	global.RegisterDatabaseCommand(name, cmd, status)
}

func init() {
	registerKeyCommands()
	registerStringCommands()
	registerSetCommands()
	registerListCommands()
	registerHashCommands()
	registerZSetCommands()
	registerBitMapCommands()
	registerBloomFilterCommands()
}
