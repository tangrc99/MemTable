package server

import (
	"MemTable/db"
	"MemTable/db/structure"
	"MemTable/logger"
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

	status ClientStatus // 状态 0 等待连接 1 正常 -1 退出 -2 异常

	db   *db.DataBase  // 数据库的序号
	exit chan struct{} // 退出标志
	res  chan []byte   // 回包

	sub chan []byte // 用于订阅通知
}

func NewClient(conn net.Conn, dbImpl *db.DataBase) *Client {
	return &Client{
		cnn:    conn,
		id:     uuid.Must(uuid.NewV1()),
		tp:     time.Now(),
		status: WAIT,
		db:     dbImpl,
		exit:   make(chan struct{}, 1),
		res:    make(chan []byte, 10),
		sub:    make(chan []byte, 10),
	}
}

func (cli *Client) UpdateTimestamp() {
	cli.tp = time.Now()
}

type ClientList struct {
	list    *structure.List
	UUIDSet map[uuid.UUID]struct{} // 用于判断是否为新链接
}

func NewClientList() *ClientList {
	return &ClientList{
		list:    structure.NewList(),
		UUIDSet: make(map[uuid.UUID]struct{}),
	}
}

func (clients *ClientList) CheckIfClientExist(id uuid.UUID) bool {
	_, exist := clients.UUIDSet[id]
	return exist
}

func (clients *ClientList) AddClientIfNotExist(cli *Client) bool {
	_, exist := clients.UUIDSet[cli.id]

	if exist {
		return false
	}

	cli.status = CONNECTED
	clients.UUIDSet[cli.id] = struct{}{}
	clients.list.PushFront(cli)
	return true
}

// removeClientWithPosition 给定客户端指针和链表位置
func (clients *ClientList) removeClientWithPosition(cli *Client, node *structure.ListNode) {
	logger.Debug("ClientList: Remove Client", cli.id)
	cli.status = EXIT
	clients.list.RemoveNode(node)
	delete(clients.UUIDSet, cli.id)
	cli.exit <- struct{}{}
}

// RemoveClient 不知道具体位置时，需要遍历
func (clients *ClientList) RemoveClient(cli *Client) {
	for node := clients.list.FrontNode(); node != nil; node = node.Next() {
		if node.Value.(*Client).id == cli.id {
			clients.removeClientWithPosition(cli, node)
			break
		}
	}
}

func (clients *ClientList) RemoveLongNotUsed(num int, d time.Duration) {

	// 早于该时间的视为过期
	expired := time.Now().Add(-1 * d)

	for node := clients.list.FrontNode(); node != nil && num >= 0; {
		cli, ok := node.Value.(*Client)

		if !ok {
			logger.Error("ClientList: type is not Client")
			next := node.Next()
			clients.list.RemoveNode(node)
			node = next

		} else if cli.tp.Before(expired) || cli.status == ERROR || cli.status == EXIT {
			// 清理过期和失效客户端
			next := node.Next()
			clients.removeClientWithPosition(cli, node)
			node = next
			num--

		} else {
			node = node.Next()
		}

	}
}

func (clients *ClientList) Size() int {
	return clients.list.Size()
}
