package main

import (
	"bytes"
	"fmt"
	"github.com/tangrc99/MemTable/client/client"
	"github.com/tangrc99/MemTable/logger"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server"
	"github.com/tangrc99/MemTable/utils/readline"
)

// RunInteractionMode 会从 AOF 或 RDB 文件中恢复数据，并且以非持久化的方式来运行。
// 可以理解为读取了某一时刻的数据库快照，并且进行 fork。
func RunInteractionMode(s *server.Server) {

	// 关闭日志输出
	e := logger.Init("", "", logger.DEBUG)
	if e != nil {
		panic(e.Error())
	}
	logger.Disable()

	s.TryRecover()

	c := readline.NewCompleter()
	client.AddRedisCompletions(c)
	t := readline.NewTerminal().WithCompleter(c)

	cli := server.NewFakeClient()

	fmt.Printf("Server runs in iteractive mode. In this mode, the database is a snapshot at startup time.\n")
	fmt.Printf("All revision of your command won't be saved!\n")

	for {
		fmt.Print("command ")
		command, aborted := t.ReadLine()
		if aborted {
			break
		}
		r := resp.PlainDataToResp(command)

		ret, _ := server.ExecCommand(s, cli, command, r.ToBytes())

		echo := resp.NewParser(bytes.NewReader(ret.ToBytes())).Parse()

		if echo.Err != nil {
			println(echo.Err.Error())
		} else {
			println(resp.ToReadableString(echo.Data, ""))
		}
	}
}
