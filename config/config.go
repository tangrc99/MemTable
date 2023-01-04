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
	MaxMemory   uint64
	AppendFsync int
	AppendOnly  bool
	GoPool      bool
	GoPoolSize  int
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
			}
		}
		if ioErr == io.EOF {
			break
		}
	}
	return nil
}

func init() {
	Conf = Config{
		ConfFile: "",
		Host:     "127.0.0.1",
		Port:     6380,
		LogDir:   "./logs",
		LogLevel: "info",
	}

	if len(os.Args) > 1 {
		Conf.ConfFile = os.Args[1]
		err := Conf.parseFile()
		if err != nil {
			panic(err)
		}
	}

}
