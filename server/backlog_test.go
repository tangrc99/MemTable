package server

import (
	"github.com/tangrc99/MemTable/config"
	"github.com/tangrc99/MemTable/logger"
	"testing"
)

func TestBackLog(t *testing.T) {

	err := logger.Init(config.Conf.LogDir, "bin.log", logger.StringToLogLevel(config.Conf.LogLevel))
	if err != nil {
		println(err.Error())
		return
	}

	s := NewServer()
	s.InitModules()
	s.standAloneToMaster()
	println(s.backLog.LowWaterLevel())

	event := &Event{raw: []byte("sdfsdfsdfds"), cli: NewClient(nil)}
	println(s.backLog.LowWaterLevel())

	s.appendBackLog(event)
	println(s.backLog.LowWaterLevel())
	println(s.backLog.HighWaterLevel())
	rd := s.backLog.Read(0, 34)
	println(string(rd))
}
