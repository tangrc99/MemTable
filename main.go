package main

import (
	"fmt"
	"github.com/tangrc99/MemTable/config"
	"github.com/tangrc99/MemTable/logger"
	"github.com/tangrc99/MemTable/server"
	_ "net/http/pprof"
	_ "runtime/trace"
	_ "time"
)

func main() {

	//go func() {
	//	fmt.Println("pprof started...")
	//	panic(http.ListenAndServe("localhost:8080", nil))
	//}()

	err := logger.Init(config.Conf.LogDir, "bin.log", logger.StringToLogLevel(config.Conf.LogLevel))
	if err != nil {
		println(err.Error())
		return
	}

	//watcher := server.ETCDWatcherInit()
	//
	//watcher.GetClusterConfig()

	s := server.NewServer(fmt.Sprintf("%s:%d", config.Conf.Host, config.Conf.Port))
	s.TryRecover()
	s.InitModules()
	//s.SendPSyncToMaster()
	//return
	s.Start()
}
