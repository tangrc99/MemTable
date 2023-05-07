package server

import (
	"github.com/tangrc99/MemTable/logger"
	"github.com/tangrc99/MemTable/resp"
	"strconv"
	"time"
)

func eval(s *Server, cli *Client, cmd [][]byte) resp.RedisData {

	e, ok := CheckCommandAndLength(cmd, "eval", 3)
	if !ok {
		return e
	}

	env.caller = cli

	body := string(cmd[1])

	keyNum, err := strconv.Atoi(string(cmd[2]))

	if err != nil {
		return resp.MakeErrorData("ERR numkeys is not an integer or out of range")
	} else if keyNum > len(cmd) {
		return resp.MakeErrorData("ERR Number of keys can't be greater than number of args")
	} else if keyNum < 0 {
		return resp.MakeErrorData("ERR Number of keys can't be negative")
	}

	if ok := checkAllKeysLocal(s, cmd[3:3+keyNum], keyNum); !ok {
		return resp.MakeErrorData("ERR script try to access non local key")
	}

	cli.blocked = true
	done := make(chan struct{}, 1)

	go func() {

		ret := evalGenericCommand(env.l, body, "", cmd[3:3+keyNum], cmd[3+keyNum:])

		cli.blocked = false
		env.caller = nil
		// 直接将结果发送给客户端
		cli.res <- &ret
		done <- struct{}{}

		// TODO: lua 命令的传播不会经过主线程，需要进行额外处理
	}()

	select {
	case <-done:

	case <-time.Tick(slowScriptTime):
		logger.Info("Lua Script: Slow Script Blocked Server")
	}

	return nil
}

func evalSha(s *Server, cli *Client, cmd [][]byte) resp.RedisData {

	e, ok := CheckCommandAndLength(cmd, "evalsha", 3)
	if !ok {
		return e
	}

	env.caller = cli
	sha := string(cmd[1])

	keyNum, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return resp.MakeErrorData("ERR numkeys is not an integer or out of range")
	} else if keyNum > len(cmd) {
		return resp.MakeErrorData("ERR Number of keys can't be greater than number of args")
	} else if keyNum < 0 {
		return resp.MakeErrorData("ERR Number of keys can't be negative")
	}

	if ok := checkAllKeysLocal(s, cmd[3:3+keyNum], keyNum); !ok {
		return resp.MakeErrorData("ERR script try to access non local key")
	}

	cli.blocked = true
	done := make(chan struct{}, 1)

	go func() {

		ret := evalGenericCommand(env.l, "", sha, cmd[3:3+keyNum], cmd[3+keyNum:])

		cli.blocked = false
		env.caller = nil
		// 直接将结果发送给客户端
		cli.res <- &ret
		done <- struct{}{}

		// TODO: lua 命令的传播不会经过主线程，需要进行额外处理
	}()

	select {
	case <-done:

	case <-time.Tick(slowScriptTime):
		logger.Info("Lua Script: Slow Script Blocked Server")
	}

	return nil
}

func script(_ *Server, _ *Client, cmd [][]byte) resp.RedisData {

	e, ok := CheckCommandAndLength(cmd, "script", 2)
	if !ok {
		return e
	}

	return scriptCommand(cmd)
}

func registerScriptCommands() {
	RegisterCommand("eval", eval, WR)
	RegisterCommand("evalsha", evalSha, WR)
	RegisterCommand("script", script, WR)
}
