package server

import (
	"MemTable/config"
	"MemTable/db"
	"MemTable/db/cmd"
	"MemTable/logger"
	"MemTable/resp"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

type Server struct {
	dbs      []*db.DataBase // 多个可以用于切换的数据库
	Chs      *db.Channels   // 订阅发布频道
	dbNum    int            //数据库数量
	clis     *ClientList    // 客户端列表
	tl       *TimeEventList // 事件链表
	commands chan *Client   // 用于解析完毕的协程同步

	url      string // 监听 url
	listener net.Listener
	quit     bool
	quitFlag chan struct{}

	aof *AOFBuffer
}

func NewServer(url string) *Server {
	n := 2

	d := make([]*db.DataBase, n)
	for i := 0; i < n; i++ {
		d[i] = db.NewDataBase()
	}

	return &Server{
		dbs:      d,
		dbNum:    n,
		Chs:      db.NewChannels(),
		clis:     NewClientList(),
		tl:       NewTimeEventList(),
		url:      url,
		commands: make(chan *Client, 1000),
		quit:     false,
		quitFlag: make(chan struct{}),
		aof:      NewAOFBuffer("/Users/tangrenchu/GolandProjects/MemTable/logs/aof"),
	}
}

func (s *Server) handleRead(conn net.Conn) {

	client := NewClient(conn)

	ch := resp.ParseStream(conn)

	// 这里会阻塞等待有数据到达
	running := true
	for running && !s.quit {

		select {
		// 等待是否有新消息到达
		case parsed := <-ch:

			if parsed.Err != nil {

				if e := parsed.Err.Error(); e == "EOF" {
					logger.Debug("Client", client.id, "Peer ShutDown Connection")
				} else {
					logger.Debug("Client", client.id, "Read Error:", e)
				}
				running = false
				break
			}

			array, ok := parsed.Data.(*resp.ArrayData)
			if !ok {
				logger.Warning("Client", client.id, "parse Command Error")
				running = false
				break
			}

			client.cmd = array.ToCommand()
			client.raw = parsed.Data.ToBytes()

			// 如果解析完毕有可以执行的命令，则发送给主线程执行
			s.commands <- client

		case r := <-client.res: // fixme : 这里的分支会导致客户端消息乱序吗

			// 将主线程的返回值写入到 socket 中
			_, err := conn.Write(r)

			if err != nil {
				logger.Warning("Client", client.id, "Write Error")
				running = false
				break
			}

		case <-client.exit:
			running = false

		case msg := <-client.msg:
			// 写入发布订阅消息
			_, err := conn.Write(msg)

			if err != nil {
				logger.Warning("Client", client.id, "Write Error")
				running = false
				break
			}
		}
	}

	// 如果是读写发生错误，需要通知事件循环来关闭连接
	if client.status != EXIT {
		// 说明这是异常退出的
		client.status = ERROR
		client.cmd = nil

		// 通知顶层
		s.commands <- client
	}

	err := conn.Close()
	if err != nil {
		return
	}

	logger.Debug("Goroutine Exit")

}

func (s *Server) eventLoop() {

	s.initTimeEvents()

	for !s.quit {
		timer := time.NewTimer(time.Second)
		select {
		case <-timer.C:
			logger.Debug("EventLoop: Timer trigger")
			// 需要完成定时任务
			s.tl.ExecuteOneIfExpire()

		case cli := <-s.commands:
			logger.Debug("EventLoop: New Event From Client", cli.id.String())

			// 底层发生异常，需要关闭客户端，或者客户端已经关闭了，那么就不处理请求了
			if cli.status == ERROR || cli.status == EXIT {
				// 释放客户端资源
				logger.Info("EventLoop: Remove Closed Client", cli.id.String())
				cli.UnSubscribeAll(s.Chs)
				s.clis.RemoveClient(cli)
				continue
			}

			// 用于判断是否为新连接
			ok := s.clis.AddClientIfNotExist(cli)

			// 如果是新连接
			if ok {
				logger.Info("EventLoop: New Client", cli.id.String())
			}

			// 更新时间戳
			cli.UpdateTimestamp()

			// 执行服务命令
			res, isWriteCommand := ExecCommand(s, cli, cli.cmd)

			if res == nil {
				// 执行数据库命令
				res, isWriteCommand = cmd.ExecCommand(s.dbs[cli.dbSeq], cli.cmd)
			}

			// 只有写命令需要完成aof持久化
			if isWriteCommand {
				s.appendAOF(cli)
			}

			// 写入回包
			cli.res <- res.ToBytes() // fixme : 这里有阻塞的风险

		}
	}

	// 处理退出逻辑
	logger.Info("Server: Ready To Shutdown")

	// 关闭监听
	_ = s.listener.Close()

	// aof 刷盘
	s.aof.Quit()

	// 关闭所有的客户端协程
	for s.clis.Size() != 0 {
		front := s.clis.list.FrontNode()
		s.clis.removeClientWithPosition(front.Value.(*Client), front)
		// 不用删除订阅
	}

	// 通知
	s.quitFlag <- struct{}{}
}

func backgroundLoop() {

	// 完成后台的任务
}

func (s *Server) acceptLoop() {
	for !s.quit {
		conn, err := s.listener.Accept()
		if err != nil {
			break
		}
		go s.handleRead(conn)
	}
	logger.Info("Server: Shutdown Listener")

}

func (s *Server) initTimeEvents() {

	// 每 300 秒清理一次过期客户端
	s.tl.AddTimeEvent(NewPeriodTimeEvent(func() {
		logger.Debug("TimeEvent: Remove Inactive Clients")
		s.clis.RemoveLongNotUsed(1, 300*time.Second)
	}, time.Now().Add(300*time.Second).Unix(), 300*time.Second,
	))

	// 过期 key 清理
	s.tl.AddTimeEvent(NewPeriodTimeEvent(func() {
		logger.Debug("TimeEvent: Remove Expired Keys")

		for _, dataBase := range s.dbs {
			// 抽样 20 个，如果有 5 个过期，则再次删除
			for dataBase.CleanTTLKeys(20) >= 5 {
			}
		}

	}, time.Now().Add(time.Second).Unix(), time.Second,
	))

	// AOF 刷盘
	s.tl.AddTimeEvent(NewPeriodTimeEvent(func() {
		logger.Debug("TimeEvent: AOF FLUSH")

		s.aof.Flush()

	}, time.Now().Add(time.Second).Unix(), time.Second,
	))

	// bgsave 持久化 trigger

	// 更新服务端信息

	// 从服务器同步操作
}

func (s *Server) Start() {

	// 初始化操作

	err := logger.Init(config.Conf.LogDir, "bin.log", logger.DEBUG)
	if err != nil {
		return
	}

	logger.Info("Server: Listen at", s.url)

	if err != nil {
		return
	}

	go s.eventLoop()

	s.recoverFromAOF("/Users/tangrenchu/GolandProjects/MemTable/logs/aof")

	// 开启监听
	s.listener, err = net.Listen("tcp", "127.0.0.1:6379")
	go s.acceptLoop()

	quit := make(chan os.Signal)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM) // 接受软中断信号并且传递到 channel

	<-quit

	// 通知主线程在完成任务后退出，防止有任务进行到一半
	s.quit = true
	<-s.quitFlag

	logger.Info("Server Shutdown...")
}

func (s *Server) appendAOF(cli *Client) {

	if len(cli.raw) <= 0 {
		return
	}

	// 只有写命令需要持久化

	if cli.dbSeq != 0 {
		// 多数据库场景需要加入数据库选择语句
		dbStr := strconv.Itoa(cli.dbSeq)
		s.aof.Append([]byte(fmt.Sprintf("*2\r\n$6\r\nselect\r\n$%d\r\n%s\r\n", len(dbStr), dbStr)))
	}

	s.aof.Append(cli.raw)
}

func (s *Server) recoverFromAOF(filename string) {
	reader, err := os.OpenFile(filename, os.O_RDONLY, 777)
	if err != nil {
		println(err.Error())
		os.Exit(-1)
	}

	client := NewClient(nil)

	ch := resp.ParseStream(reader)

	for parsedRes := range ch {

		if parsedRes.Err != nil {

			if e := parsedRes.Err.Error(); e != "EOF" {
				logger.Error("Client", client.id, "Read Error:", e)
			}
			break
		}

		array, ok := parsedRes.Data.(*resp.ArrayData)
		if !ok {
			logger.Error("Client", client.id, "parse Command Error")
			os.Exit(-1)
		}

		client.cmd = array.ToCommand()

		// 如果解析完毕有可以执行的命令，则发送给主线程执行
		s.commands <- client

		<-client.res
	}
}
