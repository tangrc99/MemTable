package server

import (
	"fmt"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/errors"
	"strings"
)

func auth(server *Server, cli *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(cmd, "auth", 2)
	if !ok {
		return e
	}

	// this is an old version command
	if len(cmd) == 2 {
		user, exist := server.acl.FindUser("default")
		if !exist {
			return resp.MakeErrorData(errors.ErrorUserNotExist("default").Error())
		}

		if !user.HasPassword() {
			return resp.MakeErrorData("ERR Client sent AUTH, but no password is set")
		}

		matched := user.IsPasswordMatch(string(cmd[1]))
		if !matched {
			return resp.MakeErrorData("ERR invalid password")
		}
		cli.user = user
		cli.auth = matched

	} else {

		user, exist := server.acl.FindUser(string(cmd[1]))
		if !exist {
			return resp.MakeErrorData(errors.ErrorUserNotExist(string(cmd[1])).Error())
		}

		if !user.HasPassword() {
			return resp.MakeErrorData("ERR Client sent AUTH, but no password is set")
		}

		matched := user.IsPasswordMatch(string(cmd[2]))
		if !matched {
			return resp.MakeErrorData("ERR invalid password")
		}
		cli.user = user
		cli.auth = matched
	}

	return resp.MakeStringData("OK")
}

func aclGenericCommand(server *Server, cli *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(cmd, "acl", 2)
	if !ok {
		return e
	}

	subcommand := strings.ToLower(string(cmd[1]))

	switch subcommand {

	case "cat":

	case "deluser":
		return aclDelUser(server, cmd)
	case "dryrun":
		return aclDryRun(server, cli, cmd)
	case "genpass":
	case "getuser":
		return aclGetUser(server, cmd)
	case "list":
		return aclList(server)
	case "load":
		return aclLoad(server)
	case "log":
	case "save":
		return aclSave(server)
	case "setuser":
		return aclSetUser(server, cmd)
	case "users":
		return aclUsers(server)
	case "whoami":
		return aclWhoAmI(cli)
	}

	return resp.MakeErrorData(errors.ErrorUnKnownSubCommand(subcommand).Error())
}

// aclCat 是 acl cat 命令的实现，用于获取 categories 或 category 中的命令
func aclCat() {

}

func aclDelUser(server *Server, cmd [][]byte) resp.RedisData {
	if len(cmd) < 3 {
		return resp.MakeErrorData("ERR wrong number of arguments for 'acl getuser' command")
	}
	userName := string(cmd[2])

	if userName == "default" {
		return resp.MakeErrorData("ERR can't delete user 'default'")
	}

	if ok := server.acl.DeleteUser(userName); !ok {
		return resp.MakeIntData(0)
	}
	return resp.MakeIntData(1)
}

// aclDryRun 是 acl dryrun 命令的实现，用于判断一个命令能否在当前用户下执行
func aclDryRun(server *Server, cli *Client, cmd [][]byte) resp.RedisData {
	if len(cmd) < 3 {
		return resp.MakeErrorData("ERR wrong number of arguments for 'acl getuser' command")
	}
	if !cli.user.IsOn() {

	}
	cmdName := string(cmd[2])
	if !cli.user.IsCommandAllowed(cmdName) {
		return resp.MakeErrorData(fmt.Sprintf("This user has no permissions to run the '%s' command", cmdName))
	}

	// TODO:

	return resp.MakeStringData("OK")
}

func aclGenPass() {
	// TODO:

}

func aclGetUser(server *Server, cmd [][]byte) resp.RedisData {
	if len(cmd) < 3 {
		return resp.MakeErrorData("ERR wrong number of arguments for 'acl getuser' command")
	}
	userName := string(cmd[2])
	user, exist := server.acl.FindUser(userName)
	if !exist {
		return resp.MakeEmptyArrayData()
	}
	return user.ToResp()
}

func aclList(server *Server) resp.RedisData {
	users := server.acl.GetAllUsers()
	ret := make([]resp.RedisData, 0, len(users))
	for _, user := range users {
		ret = append(ret, resp.MakeBulkData([]byte(user.ToString())))
	}
	return resp.MakeArrayData(ret)
}

func aclLoad(server *Server) resp.RedisData {
	ok := server.acl.ParseFromFile()
	if !ok {
		return resp.MakeErrorData("ERR There was an error trying to load the ACLs. Please check the server logs for more information")
	}
	return resp.MakeStringData("OK")
}

func aclLog() {
	// TODO:

}

func aclSave(server *Server) resp.RedisData {
	ok := server.acl.DumpToFile()
	if !ok {
		return resp.MakeErrorData("ERR There was an error trying to save the ACLs. Please check the server logs for more information")
	}
	return resp.MakeStringData("OK")
}

func aclSetUser(server *Server, cmd [][]byte) resp.RedisData {
	args := cmd[2:]
	if len(args) == 0 {
		return resp.MakeErrorData("ERR wrong number of arguments for 'acl setuser' command")
	}
	userName := strings.ToLower(string(args[0]))
	if err := server.acl.SetupUser(userName, args[1:]); err != nil {
		return resp.MakeErrorData(err.Error())
	}
	return resp.MakeStringData("OK")
}

func aclUsers(server *Server) resp.RedisData {
	users := server.acl.GetAllUserNames()
	ret := make([]resp.RedisData, 0, len(users))
	for i := range users {
		ret = append(ret, resp.MakeBulkData([]byte(users[i])))
	}
	return resp.MakeArrayData(ret)
}

func aclWhoAmI(cli *Client) resp.RedisData {
	return resp.MakeBulkData([]byte(cli.user.Name()))
}

func registerAuthCommands() {
	RegisterCommand("auth", auth, RD)
	RegisterCommand("acl", aclGenericCommand, RD)
}
