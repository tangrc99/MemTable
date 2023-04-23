package client

import (
	"errors"
	"fmt"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/global"
	"github.com/tangrc99/MemTable/utils/readline"
	"net"
	"strings"
)

type Client struct {
	url    string       // url
	host   string       // 主机名
	port   int          // 端口
	conn   net.Conn     // socket 连接
	parser *resp.Parser // 命令解析器
	flag   int          // 客户端标识
	quit   bool         // 退出标识
}

func NewClient(options ...Option) *Client {
	c := &Client{
		host: "127.0.0.1",
		port: 6379,
	}
	for _, op := range options {
		op(c)
	}
	c.url = fmt.Sprintf("%s:%d", c.host, c.port)
	return c
}

func (c *Client) Dial() error {
	if c.isConnected() {
		return nil
	}
	conn, err := net.Dial("tcp", c.url)
	if err != nil {
		return errors.New(fmt.Sprintf("Could not connect to Redis at %s: %s", c.url, err.Error()))
	}
	c.toConnected(conn)
	return nil
}

// Quit 退出客户端的交互模式
func (c *Client) Quit() {
	c.quit = true
	_ = c.conn.Close()
}

func (c *Client) Call(msg []byte) (string, error) {
	for i := 0; i < len(msg); {
		n, err := c.conn.Write(msg[i:])
		if err != nil {
			c.toDisconnected()
			return "", errors.New(fmt.Sprintf("Could not connect to Redis at %s: %s", c.url, err.Error()))
		}
		i += n
	}
	echo := c.parser.Parse()
	if echo.Abort {
	} else if echo.Err != nil {
		c.toDisconnected()
		return "", echo.Err
	}
	return resp.ToReadableString(echo.Data, ""), nil
}

func (c *Client) WaitResponse() (string, error) {
	echo := c.parser.Parse()
	if echo.Abort {
		c.toDisconnected()
	} else if echo.Err != nil {

	}
	return resp.ToReadableString(echo.Data, ""), nil
}

func (c *Client) maybeChangeStatus(command [][]byte) {

	cmdName := strings.ToLower(string(command[0]))

	switch cmdName {
	case "multi":
		c.toInTx()
	case "exec", "discard":
		c.toNotInTx()
	}

	if global.IsBlockCommand(cmdName) {
		c.toBlocked()
	}
}

func (c *Client) RunInteractiveMode() {

	completer := readline.NewCompleter()
	AddRedisCompletions(completer)

	t := readline.NewTerminal().WithHistoryLimitation(20).WithCompleter(completer)

	for !c.quit {

		// 显示前缀
		if !c.isConnected() {
			fmt.Print("not connected")
		} else {
			fmt.Print(c.url)
			if c.isInTx() {
				fmt.Print("(tx)")
			}
		}

		command, abort := t.ReadLine()

		if abort {
			return
		}

		if len(command) == 0 {
			continue
		}

		// 如果处于未连接状态，尝试进行连接
		if !c.isConnected() {
			if err := c.Dial(); err != nil {
				fmt.Printf("%s\n", err.Error())
				continue
			}
		}

		c.maybeChangeStatus(command)

		r := resp.PlainDataToResp(command)

		ret, err := c.Call(r.ToBytes())

		if err != nil {
			// TODO : 这里应该有选择地报错
			fmt.Printf("%s\n", err.Error())
		} else {
			fmt.Printf("%s\n", ret)
		}

		for c.isConnected() && c.isBlocked() {
			ret, err = c.WaitResponse()
			if err != nil {
				fmt.Printf("%s\n", err.Error())
			} else {
				fmt.Printf("%s\n", ret)
			}
		}
	}
}

func (c *Client) RunSingeMode(command []string) {
	// 如果处于未连接状态，尝试进行连接
	if !c.isConnected() {
		if err := c.Dial(); err != nil {
			fmt.Printf("%s\n", err.Error())
			return
		}
	}
	lines := make([]resp.RedisData, len(command))
	for i := range command {
		lines[i] = resp.MakeBulkData([]byte(command[i]))
	}

	msg := resp.MakeArrayData(lines).ToBytes()

	ret, err := c.Call(msg)

	if err != nil {
		// TODO : 这里应该有选择地报错
		fmt.Printf("%s\n", err.Error())
	} else {
		fmt.Printf("%s\n", ret)
	}

	for c.isConnected() && c.isBlocked() {
		ret, err = c.WaitResponse()
		if err != nil {
			fmt.Printf("%s\n", err.Error())
		} else {
			fmt.Printf("%s\n", ret)
		}
	}
}
