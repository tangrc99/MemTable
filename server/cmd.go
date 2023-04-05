package server

import (
	"fmt"
	"github.com/tangrc99/MemTable/config"
	"github.com/tangrc99/MemTable/db"
	_ "github.com/tangrc99/MemTable/db/cmd"
	"github.com/tangrc99/MemTable/logger"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/global"
	"reflect"
	"strings"
)

type ExecStatus = global.ExecStatus

const WR = global.WR
const RD = global.RD

type CommandType = global.CommandType

const CTServer = global.CTServer
const CTDatabase = global.CTDatabase

type Command = func(server *Server, cli *Client, cmd [][]byte) resp.RedisData

func RegisterCommand(name string, cmd Command, status ExecStatus) {
	global.RegisterServerCommand(name, cmd, status)
}

func init() {
	registerPubSubCommands()
	registerConnectionCommands()
	registerServerCommand()
	registerTransactionCommand()
	registerReplicationCommands()
	registerScriptCommands()
	registerClusterCommand()
	registerAuthCommands()
}

func execCommand(c global.Command, server *Server, cli *Client, cmds [][]byte) resp.RedisData {

	f := c.Function()

	if c.Type() == CTDatabase {
		df, ok := f.(func(base *db.DataBase, cmd [][]byte) resp.RedisData)
		if !ok {
			logger.Errorf("Error command type %d with %s", c.Type(), reflect.TypeOf(c.Function()).String())
			return resp.MakeErrorData("Err Server Error")
		}
		return df(server.dbs[cli.dbSeq], cmds)

	} else if c.Type() == CTServer {

		sf, ok := f.(func(server *Server, cli *Client, cmd [][]byte) resp.RedisData)
		if !ok {
			logger.Errorf("Error command type %d with %s", c.Type(), reflect.TypeOf(c.Function()).String())
			return resp.MakeErrorData("Err Server Error")
		}
		return sf(server, cli, cmds)
	}

	logger.Errorf("Unknown command type %d", c.Type())

	return resp.MakeErrorData("Err Server Error")
}

func ExecCommand(server *Server, cli *Client, cmds [][]byte, raw []byte) (ret resp.RedisData, dirty bool) {

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

	commandName := strings.ToLower(string(cmds[0]))

	// 判断命令是否存在
	c, ok := global.FindCommand(commandName)

	if !ok {
		return resp.MakeErrorData("error: unsupported command"), false
	}

	// 判断是否有权限访问
	passed := checkAuthority(cli, commandName)
	if !passed {
		return resp.MakeErrorData("ERR operation not permitted"), false
	}

	writeAllowed := !(server.role == Slave && cli != server.Master)

	if c.IsWriteCommand() && !writeAllowed {
		return resp.MakeErrorData("ERR READONLY You can't write against a read only slave"), false
	}

	// 如果正在事务中
	if cli.inTx && NotTxCommand(strings.ToLower(string(cmds[0]))) {
		cli.tx = append(cli.tx, cmds)
		cli.txRaw = append(cli.txRaw, raw)
		return resp.MakeStringData("QUEUED"), false
	}

	// check before write
	access := int64(0)
	if server.full && c.IsWriteCommand() {
		access = server.dbs[cli.dbSeq].IsKeyPermitted(string(cmds[1]))
		if access == -1 {
			// 拒绝写入
			return resp.MakeErrorData("ERR database is full"), false
		}
	}

	ret = execCommand(c, server, cli, cmds)

	// 更新 cost
	server.collectCost()
	if server.full {
		server.dbs[cli.dbSeq].Evict(access, server.cost-int64(config.Conf.MaxMemory))
	}

	return ret, c.IsWriteCommand()
}

func CheckCommandAndLength(cmd [][]byte, name string, minLength int) (resp.RedisData, bool) {
	cmdName := strings.ToLower(string((cmd)[0]))
	if cmdName != name {
		return resp.MakeErrorData("Server error"), false
	}

	if len(cmd) < minLength {
		return resp.MakeErrorData(fmt.Sprintf("ERR wrong number of arguments for '%s' command", (cmd)[0])), false
	}

	return nil, true
}

func checkAuthority(cli *Client, commandName string) bool {
	if commandName == "auth" {
		return true
	}

	if cli.auth || !cli.user.HasPassword() {
		// 防止无密码账号修改密码，影响登录状态
		cli.auth = true
		// 已经授权，检查是否符合条件
		return cli.user.IsCommandAllowed(commandName)
	}
	return false
}

func NotTxCommand(cmd string) bool {
	return cmd != "exec" && cmd != "discard" && cmd != "watch" && cmd != "multi"
}
