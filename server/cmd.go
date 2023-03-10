package server

import (
	"fmt"
	"github.com/tangrc99/MemTable/db/cmd"
	"github.com/tangrc99/MemTable/resp"
	"strconv"
	"strings"
)

type ExecStatus int

const (
	RD ExecStatus = iota
	WR
)

type Command = func(server *Server, cli *Client, cmd [][]byte) resp.RedisData

var CommandTable = make(map[string]Command)
var WriteCommands = make(map[string]struct{})

func RegisterCommand(name string, cmd Command, status ExecStatus) {
	CommandTable[name] = cmd
	if status == WR {
		WriteCommands[name] = struct{}{}
	}
}

func init() {
	RegisterPubSubCommands()
	RegisterConnectionCommands()
	RegisterServerCommand()
	RegisterTransactionCommand()
	RegisterReplicationCommands()
	RegisterScriptCommands()
	RegisterClusterCommand()
}

func ExecCommand(server *Server, cli *Client, cmds [][]byte, raw []byte) (ret resp.RedisData, isWrite bool) {

	if len(cmds) == 0 {
		return resp.MakeErrorData("error: empty command"), false
	}

	// 判断是否需要转移错误
	if allowed, err := checkCommandRunnableInCluster(server, cli, cmds); !allowed {
		return err, false
	}

	// 判断是否允许在脚本环境下运行
	if allowed := CheckCommandRunnableNow(cmds, cli); allowed == false {
		return resp.MakeErrorData("BUSY running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE"), false
	}

	_, isWriteCommand := WriteCommands[strings.ToLower(string(cmds[0]))]

	writeAllowed := !(server.role == Slave && cli != server.Master)

	if isWriteCommand && !writeAllowed {
		return resp.MakeErrorData("ERR READONLY You can't write against a read only slave"), false
	}

	// 如果正在事务中
	if cli.inTx && NotTxCommand(strings.ToLower(string(cmds[0]))) {
		cli.tx = append(cli.tx, cmds)
		cli.txRaw = append(cli.txRaw, raw)
		return resp.MakeStringData("QUEUED"), false
	}

	f, ok := CommandTable[strings.ToLower(string(cmds[0]))]

	// 如果没有匹配命令，执行数据库命令
	if !ok {

		// 数据库已经写满，检查数据库能否执行写命令
		if server.full && cmd.IsWriteCommand(string(cmds[0])) {
			selectedDB := server.dbs[cli.dbSeq]
			victims, accpeted := selectedDB.Evict(cmds[1], 1)

			// 如果有键逐出，需要持久化
			if len(victims) > 0 {
				dbStr := strconv.Itoa(cli.dbSeq)
				dels := fmt.Sprintf("*%d\r\n$3\r\ndel\r\n", len(victims)+1)

				for _, k := range victims {
					dels += fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)
				}
				// 写 aof
				server.aof.append([]byte(fmt.Sprintf("*%d\r\n$6\r\ndel\r\n$%d\r\n%s\r\n", len(dbStr), dbStr)))
				server.aof.append([]byte(dels))
				// 写 backlog
				server.appendBackLogRaw([]byte(fmt.Sprintf("*%d\r\n$6\r\ndel\r\n$%d\r\n%s\r\n", len(dbStr), dbStr)))
				server.appendBackLogRaw([]byte(dels))
			}

			if !accpeted {
				return resp.MakeErrorData("ERR Memory use is limited, no keys to evict"), false
			}
		}

		return cmd.ExecCommand(server.dbs[cli.dbSeq], cmds, writeAllowed)
	}

	return f(server, cli, cmds), isWriteCommand
}

func CheckCommandAndLength(cmd *[][]byte, name string, minLength int) (resp.RedisData, bool) {
	cmdName := strings.ToLower(string((*cmd)[0]))
	if cmdName != name {
		return resp.MakeErrorData("Server error"), false
	}

	if len(*cmd) < minLength {
		return resp.MakeErrorData(fmt.Sprintf("ERR wrong number of arguments for '%s' command", (*cmd)[1])), false
	}

	return nil, true
}

func NotTxCommand(cmd string) bool {
	return cmd != "execTX" && cmd != "discard" && cmd != "watch" && cmd != "multi"
}

func IsWriteCommand(cmdName string) bool {

	if _, ok := WriteCommands[cmdName]; ok {
		return true
	}
	return cmd.IsWriteCommand(cmdName)
}

func IsRandCommand(cmdName string) bool {

	return cmd.IsRandCommand(cmdName)
}
