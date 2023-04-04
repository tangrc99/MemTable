// Package config 主要负责读取配置文件内容
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

// Config 包含了配置文件中的所有选项
type Config struct {
	ConfFile   string
	Host       string
	Port       int
	TLSPort    int
	AuthClient bool
	CertFile   string
	KeyFile    string
	CaCertFile string
	LogDir     string
	LogLevel   string

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

	// 集群配置
	ClusterEnable bool
	ClusterName   string

	// 键置换配置
	Eviction string

	SlowLogMaxLen     int
	SlowLogSlowerThan int64
}

// Conf 变量存储从配置文件读取到的配置，如果配置不存在则使用默认配置
var Conf Config

// Error 代表了解析配置文件过程中的错误
type Error struct {
	message string
}

// Error 返回错误的具体内容
func (e *Error) Error() string {
	return e.message
}

// parseFile 用于解析配置文件，如配置文件有错误则返回 error
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
				if port > 0 && (port <= 1024 || port >= 65535) {
					return &Error{fmt.Sprintf("Listening port should between 1024 and 65535, but %d is given.", port)}
				}
				cfg.Port = port
			} else if cfgName == "tls-port" {
				port, err := strconv.Atoi(fields[1])
				if err != nil {
					return err
				}
				if port > 0 && (port <= 1024 || port >= 65535) {
					return &Error{fmt.Sprintf("TLS Listening port should between 1024 and 65535, but %d is given.", port)}
				}
				cfg.TLSPort = port
			} else if cfgName == "tls-auth-clients" {

				auth, err := strconv.ParseBool(fields[1])
				if err != nil {
					return err
				}
				cfg.AuthClient = auth

			} else if cfgName == "tls-cert-file" {

				cfg.CertFile = strings.ToLower(fields[1])

			} else if cfgName == "tls-key-file" {

				cfg.KeyFile = strings.ToLower(fields[1])

			} else if cfgName == "tls-ca-cert-file" {

				cfg.CaCertFile = strings.ToLower(fields[1])

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

			} else if cfgName == "clusterenable" {

				enable, err := strconv.ParseBool(fields[1])
				if err != nil {
					return err
				}
				cfg.ClusterEnable = enable

			} else if cfgName == "clustername" {

				cfg.ClusterName = strings.ToLower(fields[1])

			} else if cfgName == "eviction" {

				cfg.ClusterName = strings.ToLower(fields[1])

			} else if cfgName == "slowlog-log-slower-than" {

				slow, err := strconv.Atoi(fields[1])
				if err != nil {
					return err
				}
				cfg.SlowLogSlowerThan = int64(slow)

			} else if cfgName == "slowlog-max-len" {

				max, err := strconv.Atoi(fields[1])
				if err != nil {
					return err
				}
				cfg.SlowLogMaxLen = max
			}

		}
		if ioErr == io.EOF {
			break
		}
	}
	return nil
}

// defaultConf 是默认配置
var defaultConf = Config{
	ConfFile:    "",
	Host:        "127.0.0.1",
	Port:        6380,
	TLSPort:     0,
	AuthClient:  true,
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

	ClusterEnable: false,
	ClusterName:   "",

	Eviction: "no",

	SlowLogMaxLen:     100,
	SlowLogSlowerThan: 10000, // 1000 us
}

// init 函数会在包初始化阶段将配置文件内容读取到 Conf 变量中
func init() {
	// 默认的配置
	Conf = defaultConf

	for i := range os.Args {
		if os.Args[i] == "--conf" {
			Conf.ConfFile = os.Args[i+1]
			err := Conf.parseFile()
			if err != nil {
				fmt.Printf(err.Error())
				fmt.Printf("Using Default Config")
				Conf = defaultConf
			}
		}
	}

	// check tls options
	if Conf.TLSPort == Conf.Port {
		panic(fmt.Sprintf("Err tls-port %d == port %d", Conf.TLSPort, Conf.Port))
	}
	if Conf.TLSPort != 0 && Conf.CaCertFile == "" {
		panic(fmt.Sprintf("Err empty tls-ca-cert-file"))
	}
	if Conf.TLSPort != 0 && Conf.KeyFile == "" {
		panic(fmt.Sprintf("Err empty tls-key-file"))
	}
	if Conf.TLSPort != 0 && Conf.CertFile == "" {
		panic(fmt.Sprintf("Err empty tls-cert-file"))
	}

	// check cluster options
	if Conf.ClusterEnable && Conf.ClusterName == "" {
		panic(fmt.Sprintf("Err empty clustername"))
	}

	// check go pool
	if Conf.GoPoolSize < Conf.GoPoolSpawn {
		panic(fmt.Sprintf("Err gopoolsize < gopoolspawn"))
	}
	if Conf.GoPoolSize < 0 {
		panic(fmt.Sprintf("Err gopoolsize < 0"))
	}
	if Conf.GoPoolSpawn < 0 {
		panic(fmt.Sprintf("Err gopoolspawn < 0"))
	}
}
