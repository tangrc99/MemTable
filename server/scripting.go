package server

import (
	"fmt"
	"github.com/tangrc99/MemTable/logger"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/utils"
	lua "github.com/yuin/gopher-lua"
	"strconv"
	"strings"
	"time"
)

type LuaEnv struct {
	l *lua.LState

	running     bool
	writeDirty  bool
	randomDirty bool
	curScript   string
	execTime    time.Duration

	server *Server

	caller  *Client
	fakeCli *Client

	scripts map[string]string
	loaded  *lua.LTable
}

var env LuaEnv

type LuaLogLevel int

const (
	luaLogDebug LuaLogLevel = iota
	luaLogVerbose
	luaLogNotice
	luaLogWarning
)

// initLuaEnv 初始化 lua 环境
func initLuaEnv(s *Server) LuaEnv {

	L := lua.NewState()
	L.OpenLibs()

	// 载入其他库函数

	// 禁止使用 file 函数
	L.SetGlobal("loadfile", lua.LNil)
	L.SetGlobal("dofile", lua.LNil)

	// 更替 random 函数

	// 设置全局表 redis
	luaCmds := L.NewTable()

	L.SetTable(luaCmds, lua.LString("call"), L.NewFunction(luaRedisCall))

	L.SetTable(luaCmds, lua.LString("log"), L.NewFunction(luaRedisLog))
	L.SetTable(luaCmds, lua.LString("log_debug"), lua.LNumber(luaLogDebug))
	L.SetTable(luaCmds, lua.LString("log_verbose"), lua.LNumber(luaLogVerbose))
	L.SetTable(luaCmds, lua.LString("log_notice"), lua.LNumber(luaLogNotice))
	L.SetTable(luaCmds, lua.LString("log_warning"), lua.LNumber(luaLogWarning))

	L.SetTable(luaCmds, lua.LString("sha1hex"), L.NewFunction(luaRedisSha1Hex))

	L.SetGlobal("redis", luaCmds)

	// 建立全局表用于存储用户加载的脚本
	luaScripts := L.NewTable()
	L.SetGlobal("@user_script", luaScripts)

	// 设置全局表 KEYS 和 ARGV
	setGlobalArray(L, "KEYS", nil)
	setGlobalArray(L, "ARGV", nil)

	disableGlobalVariantCreation(L)

	return LuaEnv{
		l:           L,
		running:     false,
		writeDirty:  false,
		randomDirty: false,
		server:      s,
		caller:      nil,
		fakeCli:     nil,
		scripts:     make(map[string]string),
		loaded:      luaScripts,
	}
}

func disableGlobalVariantCreation(L *lua.LState) {

	// 通过设置 __newindex 和 __index 方法来控制对全局变量的访问

	s := "setmetatable(_G, {\n" +
		"            __newindex = function (t, n, v)\n" +
		"               if string.sub(n,1,2) ~= 'f_' then \n" +
		"                   error(\"Script attempted to create global variable '\"..tostring(n)..\"'\", 2) \n" +
		"               else  \n                    " +
		"                   rawset(t, n, v) \n               " +
		"               end \n" +
		"            end,\n" +
		"            \n" +
		"            __index = function (t, n, v) \n" +
		"               if string.sub(n,1,2) ~= 'f_' then \n" +
		"                    error(\"attempt to read undeclared variable '\"..tostring(n)..\"'\", 2) \n" +
		"               end \n" +
		"            end, \n" +
		"        })"

	err := L.DoString(s)
	if err != nil {
		println(err.Error())
		return
	}
}

func setGlobalArray(L *lua.LState, name string, values [][]byte) {
	array := L.NewTable()
	for n, v := range values {
		L.SetTable(array, lua.LNumber(n+1), lua.LString(v))
	}
	L.SetGlobal(name, array)
}

/* ---------------------------------------------------------------------------
 * redis command 函数实现
 * ------------------------------------------------------------------------- */

func evalGenericCommand(L *lua.LState, body, sha string, keys, argv [][]byte) resp.RedisData {

	// 查找名称与编译，将函数体封装放入 lua 环境中
	if sha == "" {
		sha = utils.Sha1([]byte(body))
	}

	fName := "f_" + sha

	// 如果 Lua 环境中没有脚本，需要编译
	if L.GetGlobal(fName) == lua.LNil {
		err := L.DoString("function f_" + sha + "() " + body + "\nend")
		if err != nil {
			println(strings.Trim(err.Error(), "\n"))
			return resp.MakeErrorData(err.Error())
		}

		env.scripts[sha] = body
	}

	// 准备 key 和 args
	setGlobalArray(L, "KEYS", keys)
	setGlobalArray(L, "ARGV", argv)

	// 初始化标识位
	env.writeDirty = false
	env.randomDirty = false
	env.running = true

	// 使用 pcall 包裹运行脚本
	L.Push(L.GetGlobal(fName))

	err := L.PCall(0, 1, nil) //FIXME: ERROR FUNCTION
	if err != nil {

		// TODO : 去除 头尾
		println(err.Error()[0:strings.Index(err.Error(), "\n")])
		return resp.MakeErrorData(err.Error())
	}

	// 获取结果
	ret := L.CheckAny(-1)

	// 清除标识位
	env.running = false

	// 最终这里需要转换为 resp 格式
	respRet := luaDataToResp(ret)

	println(string(respRet.ToBytes()))

	// 清理现场

	return respRet
}

func scriptCommand() resp.RedisData {
	// flush 删除所有脚本
	// exists 价差脚本是否存在
	// load 加载一个脚本但是不运行
	// kill 关闭正在运行的脚本

	return resp.MakeStringData("ok")
}

/* ---------------------------------------------------------------------------
 * Lua 注册函数实现
 * ------------------------------------------------------------------------- */

func luaRedisCall(L *lua.LState) int {
	/* Explicitly feed monitor here so that lua commands appear after their
	 * script command. */

	return luaRedisCallImpl(L, false)
}

func luaRedisPCall(L *lua.LState) int {
	return luaRedisCallImpl(L, true)
}

// luaRedisCallImpl implements redis.call
func luaRedisCallImpl(L *lua.LState, protected bool) int {

	argc := L.GetTop()

	if argc < 1 {
		return generateError(L, "ERR KEYS + ARGV is negative", protected)
	}

	// 准备执行脚本
	env.fakeCli = NewClient(nil)

	for i := 0; i < argc; i++ {
		env.fakeCli.cmd = append(env.fakeCli.cmd, []byte(L.CheckString(i+1)))
	}
	cmdName := string(env.fakeCli.cmd[0])

	// 检查写操作和随机操作
	if IsRandCommand(cmdName) {
		env.randomDirty = true
	}
	if IsWriteCommand(cmdName) {
		env.writeDirty = true
		if env.randomDirty == true {
			return generateError(L, "ERR EXECUTING WRITE AFTER RANDOM OPERATION", protected)
		}
	}

	// 执行命令
	ret, _ := ExecCommand(env.server, env.fakeCli, env.fakeCli.cmd)

	// resp 协议转换为 lua table
	lval := respDataToLua(ret)

	L.Push(lval)

	return 1
}

func luaRedisSha1Hex(L *lua.LState) int {
	argc := L.GetTop()

	if argc < 1 {
		return generateError(L, "ERR MISSING ARGV OF sha1hex", false)
	}

	str := L.CheckString(1)

	sha1 := utils.Sha1([]byte(str))

	L.Push(lua.LString(sha1))

	return 1
}

func luaRedisLog(L *lua.LState) int {

	argc := L.GetTop()

	if argc < 2 {
		return generateError(L, "ERR argc of log is less than 2", false)
	}

	logMsg := L.CheckString(2)

	switch (LuaLogLevel)(L.CheckInt(1)) {

	case luaLogDebug:
		logger.Debug(logMsg)

	case luaLogVerbose:
		logger.Debug(logMsg)

	case luaLogNotice:
		logger.Info(logMsg)

	case luaLogWarning:
		logger.Warning(logMsg)

	default:
		return generateError(L, fmt.Sprintf("ERR unkown loglevel: %d", L.CheckInt(1)), false)
	}

	return 1
}

func luaRedisErrorReply(L *lua.LState) int {

	argc := L.GetTop()

	if argc < 2 {
		return generateError(L, "ERR argc of error_reply is less than 1", false)
	}

	v := L.Get(1)
	if v.Type() != lua.LTString && v.Type() != lua.LTNumber {
		return generateError(L, "ERR argv of error_reply is not string or number", false)
	}

	t := L.NewTable()
	L.SetTable(t, lua.LString("err"), L.CheckAny(1))

	return 1
}

func luaRedisStatusReply(L *lua.LState) int {

	argc := L.GetTop()

	if argc < 2 {
		return generateError(L, "ERR argc of status_reply is less than 1", false)
	}

	v := L.Get(1)
	if v.Type() != lua.LTString && v.Type() != lua.LTNumber {
		return generateError(L, "ERR argv of status_reply is not string or number", false)
	}

	t := L.NewTable()
	L.SetTable(t, lua.LString("ok"), L.CheckAny(1))

	return 1
}

/* ---------------------------------------------------------------------------
 * util 函数实现
 * ------------------------------------------------------------------------- */

// generateError 会根据是否在保护模式下采取不同的处理。
// 在保护模式下如果出错，pcall 返回结果是一个表，表中字段 err 存储错误信息；
// 在非保护模式下，将会直接 raiseError。
func generateError(L *lua.LState, msg any, protected bool) int {

	raise := !protected
	errMsg := fmt.Sprintf("")

	if raise {

		L.RaiseError(errMsg)
		return 0

	}

	// 产生错误表并放入到 lua 栈顶
	ret := L.NewTable()
	L.SetTable(ret, lua.LString("err"), lua.LString(errMsg))
	L.Push(ret)

	return 1
}

// formatErrorFromLuaEnv 会截去一部分不必要的错误信息来简化输出。
// 从 gopher-lua 中抛出的错误信息格式如下：
// "<string>:n: error_msg \n stack_track information"
// 经过简化后的输出信息格式如下：
// "error_msg"
// 如果不符合该标准，将会返回原字符串
func formatErrorFromLuaEnv(errMsg string) string {

	if errMsg == "" {
		return ""
	}

	spos := strings.Index(errMsg, ": ")
	epos := strings.Index(errMsg, "\n")

	// 如果不符合该标准，返回原字符串
	if spos == -1 || epos == -1 {
		return errMsg
	}

	return errMsg[spos+2 : epos]
}

// resp -> lua
func respDataToLua(data resp.RedisData) lua.LValue {

	switch fmt.Sprintf("%T", data) {

	case "*resp.StringData":

		return lua.LString(data.ByteData())

	case "*resp.BulkData":
		return lua.LString(data.ByteData())

	case "*resp.IntData":
		n, _ := strconv.Atoi(string(data.ByteData()))
		return lua.LNumber(n)

	case "*resp.ErrorData":
		env.l.RaiseError("%s", data.ByteData())
		return lua.LNil

	case "*resp.ArrayData":

		a := data.(any)
		arr := a.(*resp.ArrayData)
		t := env.l.NewTable()
		for n, c := range arr.ToCommand()[1:] {
			env.l.SetTable(t, lua.LNumber(n+1), lua.LString(c))
		}
		return t

	case "*resp.PlainData":
		return lua.LString(data.ByteData())

	default:

		// 未知类型，报错
	}

	return lua.LNil
}

// lua -> resp
func luaDataToResp(data lua.LValue) resp.RedisData {

	// 这里只可能是 table，number，字符串类型

	switch data.Type() {

	case lua.LTFunction:
		return resp.MakeErrorData("ERR: WRONG LUA SCRIPT RETURN VALUE: FUNCTION")

	case lua.LTThread:
		return resp.MakeErrorData("ERR: WRONG LUA SCRIPT RETURN VALUE: THREAD")

	case lua.LTChannel:
		return resp.MakeErrorData("ERR: WRONG LUA SCRIPT RETURN VALUE: CHANNEL")

	case lua.LTUserData:
		return resp.MakeErrorData("ERR: WRONG LUA SCRIPT RETURN VALUE: USERDATA")

	case lua.LTNumber:
		if strings.Contains(data.String(), ".") {
			return resp.MakeStringData(data.String())
		} else {
			n := data.(lua.LNumber)
			return resp.MakeIntData(int64(n))
		}

	case lua.LTTable:

		// TODO: 检查是否是状态信息或者错误信息

		t := data.(*lua.LTable)
		n := t.Len()
		arr := make([]resp.RedisData, n)
		for i := 0; i < n; i++ {
			arr[i] = luaDataToResp(t.RawGetInt(i + 1))
		}
		return resp.MakeArrayData(arr)

	}

	return resp.MakeBulkData([]byte(data.String()))
}

func LuaTest() {

	s := NewServer("127.0.0.1:6379")
	s.InitModules()

	//evalCommand(nil, nil, [][]byte{[]byte("eval"), []byte(""), []byte("1"), []byte("k1"), []byte("argv1")})
	//
	//return
	s.dbs[0].SetKey("k1", []byte("v1"))

	script := " a = 1 " +
		"print(redis.call('keys')[1])  return {tonumber(1.1),'second'} "

	evalGenericCommand(env.l, script, "",
		[][]byte{[]byte("k1"), []byte("k2")}, [][]byte{})

	//sha := utils.Sha1([]byte(script))
	//
	//evalGenericCommand(env.l, "", sha,
	//	[][]byte{[]byte("k1"), []byte("k2")}, [][]byte{})

	//script2 := "print('a:',a) " +
	//	"redis.call('keys')  return {tonumber(1.1),'second'} "
	//
	//evalGenericCommand(env.l, script2, "",
	//	[][]byte{[]byte("k1"), []byte("k2")}, [][]byte{})
}
