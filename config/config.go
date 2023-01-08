package config

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	ConfFile string
	Host     string
	Port     int
	LogDir   string
	LogLevel string

	DataBases   int
	Timeout     int
	Daemonize   bool
	Dir         string
	MaxClients  int
	MaxMemory   uint64
	AppendFsync bool
	AppendOnly  bool
	GoPool      bool
	GoPoolSize  int
	GoPoolSpawn int
	RDBFile     string
}

var Conf Config

type Error struct {
	message string
}

func (e *Error) Error() string {
	return e.message
}

// parseFile is used to parse the config file and return error
func (cfg *Config) parseFile() error {
	fl, err := os.Open(cfg.ConfFile)
	if err != nil {
		return err
	}

	defer func() {
		err := fl.Close()
		if err != nil {
			fmt.Printf("Close config file error: %s \n", err.Error())
		}
	}()

	reader := bufio.NewReader(fl)
	for {
		line, ioErr := reader.ReadString('\n')
		if ioErr != nil && ioErr != io.EOF {
			return ioErr
		}

		if len(line) > 0 && line[0] == '#' {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) >= 2 {
			cfgName := strings.ToLower(fields[0])
			if cfgName == "host" {
				if ip := net.ParseIP(fields[1]); ip == nil {
					return &Error{fmt.Sprintf("Given ip address %s is invalid", cfg.Host)}
				}
				cfg.Host = fields[1]
			} else if cfgName == "port" {
				port, err := strconv.Atoi(fields[1])
				if err != nil {
					return err
				}
				if port <= 1024 || port >= 65535 {
					return &Error{fmt.Sprintf("Listening port should between 1024 and 65535, but %d is given.", port)}
				}
				cfg.Port = port
			} else if cfgName == "logdir" {

				cfg.LogDir = strings.ToLower(fields[1])

			} else if cfgName == "loglevel" {

				cfg.LogLevel = strings.ToLower(fields[1])

			} else if cfgName == "databases" {

				databases, err := strconv.Atoi(fields[1])
				if err != nil {
					return err
				}
				cfg.DataBases = databases

			} else if cfgName == "timeout" {
				timeout, err := strconv.Atoi(fields[1])
				if err != nil {
					return err
				}
				cfg.Timeout = timeout

			} else if cfgName == "daemonize" {

				daemonize, err := strconv.ParseBool(fields[1])
				if err != nil {
					return err
				}
				cfg.Daemonize = daemonize

			} else if cfgName == "dir" {

				cfg.Dir = fields[1]

			} else if cfgName == "maxclients" {

				maxclients, err := strconv.Atoi(fields[1])
				if err != nil {
					return err
				}
				cfg.MaxClients = maxclients

			} else if cfgName == "maxmemory" {
				maxmemory, err := strconv.Atoi(fields[1])
				if err != nil {
					return err
				}
				if maxmemory <= 0 {
					return &Error{"maxmemory < 0"}
				}
				cfg.MaxMemory = uint64(maxmemory)

			} else if cfgName == "appendfsync" {

			} else if cfgName == "appendonly" {

				appendonly, err := strconv.ParseBool(fields[1])
				if err != nil {
					return err
				}
				cfg.AppendOnly = appendonly

			} else if cfgName == "gopool" {

				gopool, err := strconv.ParseBool(fields[1])
				if err != nil {
					return err
				}
				cfg.GoPool = gopool

			} else if cfgName == "dbfilename" {

				cfg.RDBFile = fields[1]

			} else if cfgName == "gopoolsize" {

				gopoolsize, err := strconv.Atoi(fields[1])
				if err != nil {
					return err
				}
				if gopoolsize <= 0 {
					return &Error{"gopoolsize < 1000"}
				}
				cfg.GoPoolSize = gopoolsize

			} else if cfgName == "gopoolspawn" {

				gopoolspawn, err := strconv.Atoi(fields[1])
				if err != nil {
					return err
				}
				if gopoolspawn <= 0 {
					return &Error{"gopoolsize < 1000"}
				}
				cfg.GoPoolSpawn = gopoolspawn

			} else if cfgName == "maxclients" {
				maxclients, err := strconv.Atoi(fields[1])
				if err != nil {
					return err
				}
				if maxclients <= 0 {
					return &Error{"maxclients < 1000"}
				}
				cfg.MaxClients = maxclients
			}
		}
		if ioErr == io.EOF {
			break
		}
	}
	return nil
}

func init() {
	// 默认的配置
	Conf = Config{
		ConfFile:    "",
		Host:        "127.0.0.1",
		Port:        6380,
		LogDir:      "./logs",
		LogLevel:    "info",
		DataBases:   8,
		Timeout:     300,
		Daemonize:   false,
		Dir:         "./",
		MaxMemory:   1<<64 - 1,
		AppendFsync: true,
		AppendOnly:  false,
		GoPool:      true,
		GoPoolSize:  10000,
		GoPoolSpawn: 2000,
		RDBFile:     "dump.rdb",
		MaxClients:  -1,
	}

	if len(os.Args) > 1 {
		Conf.ConfFile = os.Args[1]
		err := Conf.parseFile()
		if err != nil {
			panic(err)
		}
	}

}
