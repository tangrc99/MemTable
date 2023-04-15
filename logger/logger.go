package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

// LogLevel 代表日志等级
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARNING
	ERROR
	PANIC
)

// LogConfig 存储日志的运行配置
type LogConfig struct {
	Path  string
	Name  string
	Level LogLevel
}

var (
	logFile            *os.File
	logger             *log.Logger
	logMu              sync.Mutex
	levelLabels        = []string{"debug", "info", "warning", "error", "panic"}
	logcfg             *LogConfig
	defaultCallerDepth = 2
	logPrefix          = ""
)

// StringToLogLevel 根据输入字符串返回响应的日志等级，如果无匹配，则默认为 INFO 等级日志
func StringToLogLevel(level string) LogLevel {

	switch strings.ToLower(level) {
	case "debug":
		return DEBUG
	case "info":
		return INFO
	case "warning":
		return WARNING
	case "error":
		return ERROR
	case "panic":
		return PANIC
	}

	return INFO
}

// Init 用于初始化日志运行配置
func Init(dir string, filename string, level LogLevel) error {
	var err error
	logcfg = &LogConfig{
		Path:  dir,
		Name:  filename,
		Level: INFO,
	}

	logcfg.Level = level

	if _, err = os.Stat(logcfg.Path); err != nil {
		mkErr := os.Mkdir(logcfg.Path, 0755)
		if mkErr != nil {
			return mkErr
		}
	}

	logfile := path.Join(logcfg.Path, logcfg.Name)
	logFile, err = os.OpenFile(logfile, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	writer := io.MultiWriter(os.Stdout, logFile)
	logger = log.New(writer, "", log.LstdFlags)
	return nil
}

// Disable 用于禁止日志输出
func Disable() {
	logger.SetOutput(io.Discard)
}

func setPrefix(level LogLevel) {
	_, file, line, ok := runtime.Caller(defaultCallerDepth)
	if ok {
		logPrefix = fmt.Sprintf("[%s][%s:%d] ", levelLabels[level], filepath.Base(file), line)
	} else {
		logPrefix = fmt.Sprintf("[%s] ", levelLabels[level])
	}
	logger.SetPrefix(logPrefix)
}

// Debug 写入 DEBUG 等级日志
func Debug(v ...any) {
	if logcfg.Level > DEBUG {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	setPrefix(DEBUG)
	logger.Println(v...)
}

// Debugf 写入 DEBUG 等级日志
func Debugf(format string, v ...any) {
	if logcfg.Level > DEBUG {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	setPrefix(DEBUG)
	logger.Printf(format, v...)
}

// Info 写入 INFO 等级日志
func Info(v ...any) {
	if logcfg.Level > INFO {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	setPrefix(INFO)
	logger.Println(v...)
}

// Infof 写入 INFO 等级日志
func Infof(format string, v ...any) {
	if logcfg.Level > INFO {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	setPrefix(INFO)
	logger.Printf(format, v...)
}

// Warning 写入 WARNING 等级日志
func Warning(v ...any) {
	if logcfg.Level > WARNING {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	setPrefix(WARNING)
	logger.Println(v...)
}

// Warningf 写入 WARNING 等级日志
func Warningf(format string, v ...any) {
	if logcfg.Level > WARNING {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	setPrefix(WARNING)
	logger.Printf(format, v...)
}

// Error 写入 ERROR 等级日志
func Error(v ...any) {
	if logcfg.Level > ERROR {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	setPrefix(ERROR)
	logger.Println(v...)
}

// Errorf 写入 ERROR 等级日志
func Errorf(format string, v ...any) {
	if logcfg.Level > ERROR {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	setPrefix(ERROR)
	logger.Printf(format, v...)
}

// Panic 写入 PANIC 等级日志
func Panic(v ...any) {
	if logcfg.Level > PANIC {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	setPrefix(PANIC)
	logger.Fatal(v...)
}

// Panicf 写入 PANIC 等级日志
func Panicf(format string, v ...any) {
	if logcfg.Level > PANIC {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	setPrefix(PANIC)
	logger.Fatalf(format, v...)
}
