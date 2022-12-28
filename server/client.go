package server

import (
	"MemTable/db"
	"github.com/gofrs/uuid"
	"net"
	"time"
)

type ClientStatus int

const (
	WAIT ClientStatus = iota
	CONNECTED
	EXIT
	ERROR
)

type Client struct {
	cmd [][]byte  // 当前命令
	cnn net.Conn  // 连接实例
	id  uuid.UUID // Cli 编号
	tp  time.Time // 通信时间戳

	status int // 状态 0 等待连接 1 正常 -1 退出 -2 异常

	database *db.DataBase  // 数据库的序号
	exit     chan struct{} // 退出标志
	res      chan string   // 回包
}
