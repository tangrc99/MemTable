package server

import (
	"testing"
	"time"
)

func TestScriptingGlobal(t *testing.T) {
	s := NewServer("127.0.0.1:6379")

	s.dbs[0].SetKey("k1", []byte("v1"))

	luaScript := "return { redis.call('keys')[1] , ARGV[1] } "

	r := evalGenericCommand(env.l, luaScript, "",
		[][]byte{[]byte("k1")}, [][]byte{[]byte("argv1")})

	if string(r.ToBytes()) != "*2\r\n$2\r\nk1\r\n$5\r\nargv1\r\n" {
		t.Error("Result is wrong, your result is: \n", string(r.ToBytes()))
	}
}

func TestScriptingAbort(t *testing.T) {

	_ = NewServer("127.0.0.1:6379")

	luaScript := "local clock = os.clock\n  function f_sleep(n)  -- seconds\n    local t0 = clock()\n    while clock() - t0 <= n do end\n  end\n  f_sleep(10)"

	go func() {
		time.Sleep(1 * time.Second)

		ret, ok := scriptKillCommand()
		if ok {
			t.Error("Interrupt Script Before 5 Seconds", ret)
		}

		time.Sleep(5 * time.Second)

		ret, ok = scriptKillCommand()

		if !ok {
			t.Error(ret)
		}
	}()

	r := evalGenericCommand(env.l, luaScript, "",
		[][]byte{}, [][]byte{})

	if string(r.ByteData()) != "ERR Lua script killed by user with SCRIPT KILL." {
		t.Error("Wrong Return Message: ", string(r.ByteData()))
	}
}

func TestScriptingLocal(t *testing.T) {
	_ = NewServer("127.0.0.1:6379")

	luaScript := "a = 1 return a"

	r := evalGenericCommand(env.l, luaScript, "",
		[][]byte{}, [][]byte{})

	if string(r.ByteData()) != "Script attempted to create global variable 'a'" {

		t.Error("Lua Script Created A Global Variant")
	}

}
