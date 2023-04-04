package main

import (
	"fmt"
	"github.com/tangrc99/MemTable/config"
	"github.com/tangrc99/MemTable/logger"
	"github.com/tangrc99/MemTable/server"
	_ "net/http/pprof"
	"os"
	_ "runtime/trace"
	"syscall"
	_ "time"
)

func Daemon() (int, error) {

	const daemonFlagName = "--daemon"

	isDaemon := false
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == daemonFlagName {
			isDaemon = true
		}
	}
	if isDaemon { // daemon process
		// 创建新会话，防止 ssh 启动时，因用户退出而退出
		_, err := syscall.Setsid()
		return 0, err
	}
	procPath := os.Args[0]
	// 添加"--daemon"参数
	args := make([]string, 0, len(os.Args)+1)
	args = append(args, os.Args...)
	args = append(args, daemonFlagName)

	// 把标准输入输出指向null
	fd, err := os.OpenFile("/dev/null", os.O_RDWR, 0)
	if err != nil {
		return -1, err
	}

	syscall.CloseOnExec(int(os.Stdin.Fd()))
	syscall.CloseOnExec(int(os.Stdout.Fd()))
	syscall.CloseOnExec(int(os.Stderr.Fd()))

	attr := &syscall.ProcAttr{
		Env:   os.Environ(),
		Files: []uintptr{fd.Fd(), fd.Fd(), fd.Fd()},
	}

	pid, err := syscall.ForkExec(procPath, args, attr)
	if err != nil {
		return -1, err
	}
	return pid, nil
}

func main() {

	//go func() {
	//	fmt.Println("pprof started...")
	//	panic(http.ListenAndServe("localhost:8080", nil))
	//}()

	// check daemonize
	if config.Conf.Daemonize {
		pid, err := Daemon()
		if err != nil {
			println(err.Error())
			return
		}
		if pid > 0 {
			fmt.Printf("Server run in pid %d\n", pid)
			os.Stdout.Close()
			os.Stdin.Close()
			os.Stderr.Close()
			return
		}
	}

	err := logger.Init(config.Conf.LogDir, "bin.log", logger.StringToLogLevel(config.Conf.LogLevel))
	if err != nil {
		println(err.Error())
		return
	}

	s := server.NewServer()
	s.InitModules()
	s.TryRecover()
	s.Start()

}
