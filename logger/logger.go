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

type LogLevel int
type LogConfig struct {
	Path  string
	Name  string
	Level LogLevel
}

const (
	DEBUG LogLevel = iota
	INFO
	WARNING
	ERROR
	PANIC
)

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

func Debug(v ...any) {
	if logcfg.Level > DEBUG {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	setPrefix(DEBUG)
	logger.Println(v)
}

func Info(v ...any) {
	if logcfg.Level > INFO {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	setPrefix(INFO)
	logger.Println(v)
}

func Warning(v ...any) {
	if logcfg.Level > WARNING {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	setPrefix(WARNING)
	logger.Println(v)
}

func Error(v ...any) {
	if logcfg.Level > ERROR {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	setPrefix(ERROR)
	logger.Println(v)
}

func Panic(v ...any) {
	if logcfg.Level > PANIC {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	setPrefix(PANIC)
	logger.Println(v)
}
