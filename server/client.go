package server

import (
	"github.com/gofrs/uuid"
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/db/structure"
	"github.com/tangrc99/MemTable/logger"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/acl"
	"github.com/tangrc99/MemTable/server/global"
	"net"
	"time"
	"unsafe"
)

type ClientStatus int

const (
	WAIT ClientStatus = iota
	CONNECTED
	EXIT
	ERROR
)

type Client struct {
	parser *resp.Parser

	cmd [][]byte             // 当前命令
	raw []byte               // 当前命令的 resp 格式
	res chan *resp.RedisData // 回包

	cnn   net.Conn  // 连接实例
	id    uuid.UUID // Cli 编号
	tp    time.Time // 通信时间戳
	dbSeq int

	status ClientStatus // 状态 0 等待连接 1 正常 -1 退出 -2 异常

	pipelined bool

	user *acl.User // 当前客户端的登录用户，默认为 default
	auth bool      // 当前用户是否完成了授权

	// 发布订阅
	chs map[string]struct{} //订阅频道
	msg chan []byte         // 用于订阅通知

	// 事务
	inTx    bool             // 是否处于事务中
	tx      [][][]byte       // 用于解析后的命令
	txRaw   [][]byte         // 解析前的命令
	watched map[int][]string //记录监控的键值
	revised bool             //监控是否被修改

	// 阻塞监听
	blocked   bool // 客户端是否执行阻塞等待的命令
	monitored bool

	// 主从复制
	SlaveStatus
}

func NewClient(conn net.Conn) *Client {
	return &Client{
		parser:  resp.NewParser(conn),
		cnn:     conn,
		id:      uuid.Must(uuid.NewV1()),
		tp:      global.Now,
		status:  WAIT,
		dbSeq:   0,
		res:     make(chan *resp.RedisData, 10),
		user:    acl.DefaultUser(),
		auth:    false,
		blocked: false,
	}
}

// NewFakeClient 创建一个无连接的，具有最高权限的客户端
func NewFakeClient() *Client {
	return &Client{
		id:     uuid.Must(uuid.NewV1()),
		status: CONNECTED,
		dbSeq:  0,
		res:    make(chan *resp.RedisData, 10),
		auth:   true,
		user:   acl.ManageUser(),
	}
}

func (cli *Client) ParseStream() *resp.ParsedRes {
	return cli.parser.Parse()
}

func (cli *Client) UpdateTimestamp(tp time.Time) {
	cli.tp = tp
}

func (cli *Client) Subscribe(chs *db.Channels, channel string) int {

	if cli.chs == nil {
		cli.chs = make(map[string]struct{})
		cli.msg = make(chan []byte, 10)
	}

	chs.Subscribe(channel, cli.id.String(), &cli.msg)
	cli.chs[channel] = struct{}{}
	return len(cli.chs)
}

func (cli *Client) UnSubscribe(chs *db.Channels, channel string) int {
	chs.UnSubscribe(channel, cli.id.String())
	delete(cli.chs, channel)
	return len(cli.chs)
}

func (cli *Client) UnSubscribeAll(chs *db.Channels) {
	for channel := range cli.chs {
		chs.UnSubscribe(channel, cli.id.String())
	}
	cli.chs = make(map[string]struct{})
}

func (cli *Client) InitTX() {
	cli.inTx = true
	cli.tx = make([][][]byte, 0, 20)
	cli.txRaw = make([][]byte, 0, 20)
}

func (cli *Client) InitWatchers() {
	if cli.watched == nil {
		cli.watched = make(map[int][]string)
		cli.revised = false
	}
}

func (cli *Client) ClearWatchers() {
	cli.watched = nil
	cli.revised = false
}

// IsRemovable 用于判断当前客户端是否能够被驱逐。当客户端处于事务、监控、主从复制状态下是无法驱逐的。
func (cli *Client) IsRemovable() bool {
	return !cli.inTx && !cli.monitored && cli.slaveStatus == slaveNot
}

func (cli *Client) Cost() int64 {
	return int64(unsafe.Sizeof(Client{}))
}

type ClientList struct {
	list    *structure.List
	UUIDSet map[uuid.UUID]*structure.ListNode // 用于判断是否为新链接
}

func NewClientList() *ClientList {
	return &ClientList{
		list:    structure.NewList(),
		UUIDSet: make(map[uuid.UUID]*structure.ListNode),
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
	// 将客户端加入到链表头
	clients.list.PushFront(cli)
	clients.UUIDSet[cli.id] = clients.list.FrontNode()
	return true
}

// removeClientWithPosition 给定客户端指针和链表位置，删除逻辑最终定位到这里
func (clients *ClientList) removeClientWithPosition(cli *Client, node *structure.ListNode) {
	logger.Debug("ClientList: Remove Client", cli.id)
	cli.status = EXIT
	cli.parser.Stop()
	clients.list.RemoveNode(node)
	delete(clients.UUIDSet, cli.id)
	_ = cli.cnn.Close()
}

// RemoveClient 不知道具体位置时，需要遍历
func (clients *ClientList) RemoveClient(cli *Client) {

	if cli == nil {
		return
	}

	node, exist := clients.UUIDSet[cli.id]
	if !exist {
		return
	}

	clients.removeClientWithPosition(cli, node)
}

// RemoveLongNotUsed 会尝试移除活跃时大于 d 的客户端连接，在遍历 maxTraverse 或删除 maxRemove 后函数停止。
func (clients *ClientList) RemoveLongNotUsed(maxRemove, maxTraverse int, d time.Duration) {

	// 早于该时间的视为过期
	expired := global.Now.Add(-1 * d)

	// 客户端列表尾端的时间戳会减小
	for node := clients.list.BackNode(); node != nil && maxRemove >= 0 && maxTraverse >= 0; {
		cli, ok := node.Value.(*Client)

		if !ok {

			logger.Error("ClientList: type is not Client")
			prev := node.Prev()
			clients.list.RemoveNode(node)
			node = prev

		} else if cli.IsRemovable() && cli.tp.Before(expired) {
			// 清理过期和失效客户端
			prev := node.Prev()
			clients.removeClientWithPosition(cli, node)
			node = prev
			maxRemove--

		} else {
			node = node.Prev()
		}
		maxTraverse--
	}
}

func (clients *ClientList) Size() int {
	return clients.list.Size()
}

func (clients *ClientList) UpdateTimestamp(cli *Client) {

	if cli == nil {
		return
	}

	node, exist := clients.UUIDSet[cli.id]
	if !exist {
		return
	}

	// 更新客户端列表，并且将其移动到首部
	cli.tp = global.Now
	clients.list.RemoveNode(node)
	clients.list.PushFront(cli)
}

func (clients *ClientList) Cost() int64 {
	return clients.list.Cost() + int64(24*clients.Size())
}
