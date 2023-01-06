package server

import (
	"MemTable/logger"
	"MemTable/resp"
	"fmt"
	"os"
	"strconv"
)

func (s *Server) appendAOF(cli *Client) {

	if s.aof == nil || !s.aofEnabled {
		return
	}

	if len(cli.raw) <= 0 {
		return
	}

	// 只有写命令需要持久化

	//if cli.dbSeq != 0 {
	// 多数据库场景需要加入数据库选择语句
	dbStr := strconv.Itoa(cli.dbSeq)
	s.aof.Append([]byte(fmt.Sprintf("*2\r\n$6\r\nselect\r\n$%d\r\n%s\r\n", len(dbStr), dbStr)))
	//}

	s.aof.Append(cli.raw)
}

func (s *Server) recoverFromAOF(filename string) {

	reader, err := os.OpenFile(filename, os.O_RDONLY, 777)
	if err != nil {
		logger.Warning("AOF: File Not Exists")
		return
	}

	client := NewClient(nil)

	ch := resp.ParseStream(reader)

	for parsedRes := range ch {

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
		_, _ = ExecCommand(s, client, client.cmd)

	}
}
