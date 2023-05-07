package main

import (
	"fmt"
	"github.com/tangrc99/MemTable/client/client"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	Version = "unknown"
)

func Helper() {
	fmt.Printf("memtable-cli v%s\n\n", Version)
	fmt.Printf("Usage: memtable-cli [OPTIONS] [cmd [arg [arg ...]]]\n")

	format := "  -%-20s %s\n"

	fmt.Printf(format, "h,--host <host>", "Connect to server with host <host>.")
	fmt.Printf(format, "p,--port <port>", "Connect to server with port <port>.")
	fmt.Printf(format, "r <repeat>", "Execute specified command N times.")
	fmt.Printf(format, "i <interval>", "When -r is used, waits <interval> seconds per command.")
	fmt.Printf(format, "x", "Read last argument from STDIN.")

	fmt.Printf(format, "-help", "Output this help and exit.")
	fmt.Printf(format, "-version", "Output version.")
}

type RunMode = int

const (
	// Interactive 以交互模式运行客户端，默认设置
	Interactive RunMode = iota
	// Single 以非交互模式运行客户端
	Single
	// ReadStdIn 从标准输入读取参数
	ReadStdIn
	// Latency 以延迟测试模式启动客户端
	Latency
)

var repeated = 1
var interval = 0.0
var mode = Interactive

func ParseArgs() (ops []client.Option, commands []string) {

	for i := 1; i < len(os.Args); i++ {
		switch strings.ToLower(os.Args[i]) {

		case "--host", "-h":
			ops = append(ops, client.WithHost(os.Args[i+1]))
			i++

		case "--port", "-p":
			port, err := strconv.Atoi(os.Args[i+1])
			if err != nil {
				panic(err.Error())
			}
			ops = append(ops, client.WithPort(port))
			i++

		case "--help":
			Helper()
			os.Exit(0)

		case "--version":
			fmt.Printf("memtable-cli v%s\n\n", Version)
			os.Exit(0)

		case "-r":
			r, err := strconv.Atoi(os.Args[i+1])
			if err != nil {
				panic(err.Error())
			}
			repeated = r
			i++

		case "-i":
			inter, err := strconv.ParseFloat(os.Args[i+1], 32)
			if err != nil {
				panic(err.Error())
			}
			interval = inter
			i++

		case "-x":
			mode = ReadStdIn

		case "--latency":
			mode = Latency

		default:

			if os.Args[i][0] == '-' {
				fmt.Printf("Unknown args '%s', use 'memtable-cli --help' for help\n", os.Args[i])
				os.Exit(0)
			}

			if mode != ReadStdIn {
				mode = Single
			}
			return ops, os.Args[i:]
		}
	}
	return ops, []string{}
}

func main() {

	ops, commands := ParseArgs()
	cli := client.NewClient(ops...)

	switch mode {

	// 交互模式运行
	case Interactive:
		_ = cli.Dial()
		cli.RunInteractiveMode()

	// -x 命令需要从标准输入读取数据
	case ReadStdIn:
		input := ""
		_, _ = fmt.Scanf("%s", &input)
		commands = append(commands, input)
		fallthrough

	// 非交互模式运行
	// WARNING: 该分支必须在 ReadStdIn 后面
	case Single:
		err := cli.Dial()
		if err != nil {
			fmt.Printf("%s\n", err.Error())
		}
		for i := 0; i < repeated; i++ {
			cli.RunSingeMode(commands)

			// 最后一次不需要等待
			if i != repeated-1 {
				time.Sleep(time.Duration(interval) * time.Second)
			}
		}

	// 测试延迟
	case Latency:

		for {
			ret := client.TestDelayByInterval(cli, 1000)
			ret.Print()
		}

	}

	return
}
