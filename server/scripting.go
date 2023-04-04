package server

import (
	"context"
	"fmt"
	"github.com/tangrc99/MemTable/logger"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/global"
	"github.com/tangrc99/MemTable/utils"
	lua "github.com/yuin/gopher-lua"
	"strconv"
	"strings"
	"sync"
	"time"
)

/* ---------------------------------------------------------------------------
* lua 环境变量
* ------------------------------------------------------------------------- */

type LuaEnv struct {
	l *lua.LState

	running      bool
	writeDirty   bool
	writeFlagMtx sync.Mutex // writeDirty 可能会被并发访问
	randomDirty  bool
	curScript    string

	startTime time.Time
	execTime  time.Duration

	cancelFunc context.CancelFunc

	server *Server

	caller  *Client
	fakeCli *Client

	scripts map[string]string
	loaded  *lua.LTable
}

// 当前脚本的运行环境
var env LuaEnv

// 允许在脚本超时后运行的命令
var allowedCommands = make(map[string]struct{})

// LuaLogLevel 是 lua 脚本中使用的 log level
type LuaLogLevel int

const (
	luaLogDebug LuaLogLevel = iota
	luaLogVerbose
	luaLogNotice
	luaLogWarning
)

// 脚本的超时时间
const slowScriptTime = 5 * time.Second

/* ---------------------------------------------------------------------------
* 环境初始化
* ------------------------------------------------------------------------- */

// initLuaEnv 初始化 lua 环境
func initLuaEnv(s *Server) LuaEnv {

	L := lua.NewState()

	// 载入库函数
	loadLibs(L)

	// 关闭不允许使用的功能
	unloadUnSupportedLibs(L)

	// 更替 random 函数

	// 设置全局表 redis
	luaRedisTable := L.NewTable()

	L.SetTable(luaRedisTable, lua.LString("call"), L.NewFunction(luaRedisCall))
	L.SetTable(luaRedisTable, lua.LString("pcall"), L.NewFunction(luaRedisPCall))

	L.SetTable(luaRedisTable, lua.LString("log"), L.NewFunction(luaRedisLog))
	L.SetTable(luaRedisTable, lua.LString("log_debug"), lua.LNumber(luaLogDebug))
	L.SetTable(luaRedisTable, lua.LString("log_verbose"), lua.LNumber(luaLogVerbose))
	L.SetTable(luaRedisTable, lua.LString("log_notice"), lua.LNumber(luaLogNotice))
	L.SetTable(luaRedisTable, lua.LString("log_warning"), lua.LNumber(luaLogWarning))

	L.SetTable(luaRedisTable, lua.LString("sha1hex"), L.NewFunction(luaRedisSha1Hex))
	L.SetTable(luaRedisTable, lua.LString("error_reply"), L.NewFunction(luaRedisErrorReply))
	L.SetTable(luaRedisTable, lua.LString("status_reply"), L.NewFunction(luaRedisStatusReply))

	L.SetGlobal("redis", luaRedisTable)

	// 建立全局表用于存储用户加载的脚本
	luaScripts := L.NewTable()
	L.SetGlobal("@user_script", luaScripts)

	// 设置全局表 KEYS 和 ARGV
	setGlobalArray(L, "KEYS", nil)
	setGlobalArray(L, "ARGV", nil)

	disableGlobalVariantCreation(L)

	registerCommandAvailableDuringScriptRunning()

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

func loadLibs(L *lua.LState) {
	L.OpenLibs()
	//luaLoadLib(lua, "cjson", luaopen_cjson);
	//luaLoadLib(lua, "struct", luaopen_struct);
	//luaLoadLib(lua, "cmsgpack", luaopen_cmsgpack);
	//luaLoadLib(lua, "bit", luaopen_bit);
}

func unloadUnSupportedLibs(L *lua.LState) {

	// 关闭 gopher-lua 中的 channel 库
	L.SetGlobal(lua.ChannelLibName, lua.LNil)

	// 禁止使用 file 函数
	L.SetGlobal("loadfile", lua.LNil)
	L.SetGlobal("dofile", lua.LNil)
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

func registerCommandAvailableDuringScriptRunning() {

	allowedCommands["multi"] = struct{}{}
	allowedCommands["discard"] = struct{}{}
	allowedCommands["watch"] = struct{}{}
	allowedCommands["unwatch"] = struct{}{}
	allowedCommands["ping"] = struct{}{}
}

/* ---------------------------------------------------------------------------
* command 函数实现
* ------------------------------------------------------------------------- */

func evalGenericCommand(L *lua.LState, body, sha string, keys, argv [][]byte) resp.RedisData {

	// 查找名称与编译，将函数体封装放入 lua 环境中
	if sha == "" {
		sha = utils.Sha1([]byte(body))
	}

	fName := "f_" + sha

	// 如果 Lua 环境中没有脚本，需要编译
	if L.GetGlobal(fName) == lua.LNil {

		// 进行 lua 脚本的编译
		err := L.DoString("function f_" + sha + "() " + body + "\nend")
		if err != nil {
			fmtErr := formatErrorFromLuaEnv(err.Error())
			return resp.MakeErrorData(fmtErr)
		}

		env.scripts[sha] = body
	}

	// 准备 key 和 args
	setGlobalArray(L, "KEYS", keys)
	setGlobalArray(L, "ARGV", argv)

	initFlags(L, fName)
	defer clearFlags(L)

	// 使用 pcall 包裹运行脚本
	L.Push(L.GetGlobal(fName))

	err := L.PCall(0, 1, nil) //FIXME: ERROR FUNCTION
	if err != nil {
		fmtErr := formatErrorFromLuaEnv(err.Error())
		if fmtErr == "context canceled" {
			fmtErr = "ERR Lua script killed by user with SCRIPT KILL."
		}
		return resp.MakeErrorData(fmtErr)
	}

	// 获取结果
	ret := L.CheckAny(-1)

	// 最终这里需要转换为 resp 格式
	respRet := luaDataToResp(ret)

	//TODO: 清理现场

	return respRet
}

func scriptFlushCommand() bool {

	for sha := range env.scripts {
		env.l.SetGlobal(sha, lua.LNil)
	}

	env.scripts = make(map[string]string)

	return true
}

func scriptExistCommand(sha [][]byte) int {

	existNum := 0

	// 脚本在编译完成后才会放入 scripts 表，只用检查该表
	for i := range sha {
		if _, exist := env.scripts[string(sha[i])]; exist {
			existNum++
		}
	}

	return existNum
}

// scriptLoadCommand 实现了 script load
func scriptLoadCommand(body string) (string, bool) {
	// 查找名称与编译，将函数体封装放入 lua 环境中
	L := env.l

	sha := utils.Sha1([]byte(body))

	fName := "f_" + sha

	// 如果 Lua 环境中没有脚本，需要编译
	if L.GetGlobal(fName) == lua.LNil {
		err := L.DoString("function f_" + sha + "() " + body + "\nend")
		if err != nil {
			fmtErr := formatErrorFromLuaEnv(err.Error())
			return fmtErr, false
		}

		env.scripts[sha] = body
	}

	return sha, true
}

func scriptKillCommand() (string, bool) {

	// 当前没有脚本运行
	if env.running == false {
		return "No scripts in execution right now", false
	}

	if env.execTime = time.Since(env.startTime); env.execTime < slowScriptTime {
		return "current script is not a slow script", false
	}

	// kill 命令可能会与 lua 脚本并发运行，需要加锁保护
	env.writeFlagMtx.Lock()
	defer env.writeFlagMtx.Unlock()

	// 已经写入不能够停止
	if env.writeDirty == true {
		return "Sorry the script already executed write commands against the dataset." +
			" You can either wait the script termination or kill the server in a hard way using the SHUTDOWN NOSAVE command.", false
	}

	// 取消运行
	env.cancelFunc()

	return "OK", true
}

func scriptCommand(cmd [][]byte) resp.RedisData {

	cmdName := string(cmd[1])

	if env.running && cmdName != "kill" {
		return resp.MakeErrorData("ERR Script in execution right now")
	}

	switch cmdName {
	case "flush":
		scriptFlushCommand()
		return resp.MakeStringData("OK")

	case "exists":
		num := scriptExistCommand(cmd[2:])
		return resp.MakeIntData(int64(num))

	case "load":

		if len(cmd) < 3 {
			return resp.MakeErrorData("ERR Missing script")
		}

		ret, ok := scriptLoadCommand(string(cmd[2]))
		if !ok {
			return resp.MakeErrorData("ERR load failed" + ret)
		}
		return resp.MakeBulkData([]byte(ret))

	case "kill":

		ret, ok := scriptKillCommand()
		if !ok {
			return resp.MakeErrorData("ERR kill failed" + ret)
		}
		return resp.MakeStringData("OK")
	}

	// 如果所有的分支都没有匹配，那么命令格式有问题
	return resp.MakeErrorData("ERR unsupported command 'script " + cmdName + "'")
}

/* ---------------------------------------------------------------------------
* redis 注册函数实现
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

	for i := 0; i < argc; i++ {
		env.fakeCli.cmd = append(env.fakeCli.cmd, []byte(L.CheckString(i+1)))
	}
	cmdName := string(env.fakeCli.cmd[0])

	// 检查写操作和随机操作
	if global.IsRandCommand(cmdName) {
		env.randomDirty = true
	}
	if global.IsWriteCommand(cmdName) {
		env.writeFlagMtx.Lock()
		defer env.writeFlagMtx.Unlock()
		env.writeDirty = true
		if env.randomDirty == true {
			return generateError(L, "ERR EXECUTING WRITE AFTER RANDOM OPERATION", false)
		}
	}

	// 执行命令
	ret, _ := ExecCommand(env.server, env.fakeCli, env.fakeCli.cmd, env.fakeCli.raw)

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
func generateError(L *lua.LState, msg string, protected bool) int {

	raise := !protected
	errMsg := fmt.Sprintf(msg)

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

		t := data.(*lua.LTable)

		// 检查是否是状态信息
		if v := t.RawGetString("ok"); v != lua.LNil {
			// 状态信息必须为 string 或 number 类型
			if v.Type() != lua.LTString || v.Type() != lua.LTNumber {
				return resp.MakeErrorData("ERR content of ok/status_reply is not string or number")
			}
			return resp.MakeStringData(v.String())
		}

		// 检查是否是错误信息
		if v := t.RawGetString("err"); v != lua.LNil {
			// 状态信息必须为 string 或 number 类型
			if v.Type() != lua.LTString || v.Type() != lua.LTNumber {
				return resp.MakeErrorData("ERR content of err/err_reply is not string or number")
			}
			return resp.MakeStringData(v.String())
		}

		// 其他类型信息
		n := t.Len()
		arr := make([]resp.RedisData, n)
		for i := 0; i < n; i++ {
			arr[i] = luaDataToResp(t.RawGetInt(i + 1))
		}
		return resp.MakeArrayData(arr)

	}

	return resp.MakeBulkData([]byte(data.String()))
}

func getLuaScriptBySha1(sha string) (string, bool) {

	body, exist := env.scripts[sha]
	return body, exist
}

// initFlags 初始化 Lua 环境运行标志
func initFlags(L *lua.LState, fName string) {

	// 初始化 fake client
	env.fakeCli = NewClient(nil)

	// 初始化超时处理
	ctx, cancel := context.WithCancel(context.Background())
	L.SetContext(ctx)
	env.cancelFunc = cancel

	// 初始化标识位
	env.writeDirty = false
	env.randomDirty = false
	env.running = true
	env.curScript = fName
	env.startTime = global.Now
	env.execTime = 0
}

// clearFlags 清除 Lua 环境运行标志
func clearFlags(_ *lua.LState) {
	// 清除标识位
	env.running = false
	env.caller = nil
	env.randomDirty = false
	env.writeDirty = false
	env.curScript = ""
	env.execTime = 0
	env.cancelFunc = func() {}
}

// CheckCommandRunnableNow 判断当前的命令是否允许执行。当 Lua 脚本执行时，只有一部分命令被允许执行。
func CheckCommandRunnableNow(cmdName [][]byte, cli *Client) bool {

	if env.caller == cli || env.fakeCli == cli {
		return true
	}

	if env.running == false {
		return true
	}

	// 特殊情况，只允许带特定参数的命令
	if len(cmdName) > 1 {
		if strings.ToLower(string(cmdName[0])) == "shutdown" && strings.ToLower(string(cmdName[1])) == "nosave" {
			return true
		}
		if strings.ToLower(string(cmdName[0])) == "script" && strings.ToLower(string(cmdName[1])) == "kill" {
			return true
		}
	}

	// 允许的其他少部分命令
	_, exist := allowedCommands[string(cmdName[0])]
	return exist
}
