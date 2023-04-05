package server

import (
	"fmt"
	"github.com/tangrc99/MemTable/logger"
	"github.com/tangrc99/MemTable/resp"
	"os"
	"strconv"
)

func (s *Server) appendAOF(event *Event) {

	if s.aof == nil || !s.aofEnabled {
		return
	}

	if len(event.raw) <= 0 {
		return
	}

	// 只有写命令需要持久化

	if event.cli.dbSeq != 0 {
		// 多数据库场景需要加入数据库选择语句
		dbStr := strconv.Itoa(event.cli.dbSeq)
		s.aof.append([]byte(fmt.Sprintf("*2\r\n$6\r\nselect\r\n$%d\r\n%s\r\n", len(dbStr), dbStr)))
	}

	s.aof.append(event.raw)
}

func (s *Server) recoverFromAOF(filename string) {

	reader, err := os.OpenFile(filename, os.O_RDONLY, 777)
	if err != nil {
		logger.Warning("AOF: File Not Exists")
		return
	}

	client := NewFakeClient()

	parser := resp.NewParser(reader)

	selected := false

	for {

		parsedRes := parser.Parse()

		if parsedRes.Err != nil {

			if e := parsedRes.Err.Error(); e != "EOF" {
				logger.Error("Client", client.id, "Read Error:", e)
			}
			break
		}

		array, ok := parsedRes.Data.(*resp.ArrayData)
		if !ok {
			logger.Error("Client", client.id, "parse Command Error")
			// aof 文件有损坏
			os.Exit(-1)
		}

		client.cmd = array.ToCommand()

		// 执行服务命令
		_, _ = ExecCommand(s, client, client.cmd, client.raw)

		if !selected && client.dbSeq > 0 {
			selected = true
		} else {
			selected = false
			client.dbSeq = 0
		}

	}
}
