package main

import (
	"fmt"
	"github.com/tangrc99/MemTable/client/client"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
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

var repeated = 1
var interval = 0.0
var readFromStdIn = false

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
			readFromStdIn = true

		default:

			if os.Args[i][0] == '-' {
				fmt.Printf("Unknown args '%s', use 'memtable-cli --help' for help\n", os.Args[i])
				os.Exit(0)
			}

			return ops, os.Args[i:]
		}
	}
	return ops, []string{}
}

func main() {

	ops, commands := ParseArgs()
	cli := client.NewClient(ops...)

	// -x 命令需要从标准输入读取数据
	if readFromStdIn {
		input := ""
		_, _ = fmt.Scanf("%s", &input)
		commands = append(commands, input)
	}

	// 非交互模式运行
	if len(commands) > 0 {
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
		return
	}

	// 交互模式运行
	signal.Ignore(syscall.SIGINT, syscall.SIGTERM)
	_ = cli.Dial()
	cli.RunInteractiveMode()

	return
}
