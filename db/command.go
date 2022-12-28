package db

import "MemTable/resp"

type Command = func(base *DataBase, cmd [][]byte) resp.RedisData

var CommandTable = make(map[string]Command)

func RegisterCommand(name string, cmd Command) {
	CommandTable[name] = cmd
}
