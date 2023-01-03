package server

import (
	"MemTable/db"
	"MemTable/db/cmd"
	"MemTable/logger"
	"MemTable/resp"
	"MemTable/utils/gopool"
	"fmt"
	"net"
	"os"
	"os/signal"
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

	sts *Status
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
		commands: make(chan *Client, 10000),
		quit:     false,
		quitFlag: make(chan struct{}),
		aof:      NewAOFBuffer("/Users/tangrenchu/GolandProjects/MemTable/logs/aof"),
		sts:      NewStatus(),
	}
}

func (s *Server) handleRead(conn net.Conn) {

	client := NewClient(conn)

	ch := resp.ParseStream(conn)

	//pipelined := 0

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

			if plain, ok := parsed.Data.(*resp.PlainData); ok {

				//pipelined++
				client.cmd = plain.ToCommand()
				client.raw = parsed.Data.ToBytes()

			} else if array, ok := parsed.Data.(*resp.ArrayData); ok {

				client.cmd = array.ToCommand()
				client.raw = parsed.Data.ToBytes()

			} else {

				logger.Warning("Client", client.id, "parse Command Error")
				running = false
				break
			}

			// 如果解析完毕有可以执行的命令，则发送给主线程执行
			s.commands <- client

			//		case r := <-client.res: // fixme : 这里的分支会导致客户端消息乱序吗

			// 使用 select 防止协程无法释放
			select {

			case <-client.exit:
				running = false

			case r := <-client.res:

				//if pipelined > 0 {
				//	r = resp.MakePlainData(string(r.ByteData()))
				//	pipelined--
				//}

				// 将主线程的返回值写入到 socket 中
				_, err := conn.Write(r.ToBytes())

				if err != nil {
					logger.Warning("Client", client.id, "Write Error")
					running = false
					break
				}
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

		timer := time.NewTimer(100 * time.Millisecond)

		select {

		case <-timer.C:
			logger.Debug("EventLoop: Timer trigger")
			// 需要完成定时任务
			s.tl.ExecuteManyDuring(25 * time.Millisecond)
			//s.tl.ExecuteOneIfExpire()

		case cli := <-s.commands:
			logger.Debug("EventLoop: New Event From Client", cli.id.String())

			// todo:  关闭使用定时队列来实现
			// 底层发生异常，需要关闭客户端，或者客户端已经关闭了，那么就不处理请求了
			if cli.status == ERROR || cli.status == EXIT {
				// 释放客户端资源
				logger.Debug("EventLoop: Remove Closed Client", cli.id.String())
				cli.UnSubscribeAll(s.Chs)
				s.clis.RemoveClient(cli)
				continue
			}

			// 用于判断是否为新连接
			ok := s.clis.AddClientIfNotExist(cli)

			// 如果是新连接
			if ok {
				logger.Debug("EventLoop: New Client", cli.id.String())
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
			if isWriteCommand && fmt.Sprintf("%T", res) != "*resp.ErrorData" {
				s.appendAOF(cli)
			}

			// 写入回包
			cli.res <- res

		}
	}

	// 处理退出逻辑
	logger.Info("Server: Ready To Shutdown")

	// 关闭监听
	_ = s.listener.Close()

	// aof 刷盘
	s.aof.Quit()
	// rdb 持久化
	//s.RDB()

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

	pool := gopool.NewPool(10000, 0, 2000)

	logger.Info("Server: Start Listen")

	for !s.quit {
		conn, err := s.listener.Accept()
		if err != nil {
			break
		}
		pool.Schedule(func() {
			s.handleRead(conn)
		})
		//go s.handleRead(conn)
	}
	logger.Info("Server: Shutdown Listener")

}

func (s *Server) initTimeEvents() {

	// 每 300 秒清理一次过期客户端
	s.tl.AddTimeEvent(NewPeriodTimeEvent(func() {
		logger.Debug("TimeEvent: Remove Inactive Clients")

		s.clis.RemoveLongNotUsed(1, 300*time.Second)

	}, time.Now().Add(10*time.Second).Unix(), 10*time.Second,
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
		//s.aof.Sync()

	}, time.Now().Add(time.Second).Unix(), time.Second,
	))

	// bgsave 持久化 trigger

	// 更新服务端信息
	s.tl.AddTimeEvent(NewPeriodTimeEvent(func() {
		logger.Debug("TimeEvent: Update Status")

		s.sts.UpdateStatus()

	}, time.Now().Add(time.Second).Unix(), time.Second,
	))

	// 从服务器同步操作
}

func (s *Server) Start() {

	// 初始化操作

	logger.Info("Server: Listen at", s.url)

	s.recoverFromAOF("/Users/tangrenchu/GolandProjects/MemTable/logs/aof")

	go s.eventLoop()

	// 开启监听
	var err error
	s.listener, err = net.Listen("tcp", "127.0.0.1:6379")
	if err != nil {
		logger.Error("Server:", err.Error())
	}

	go s.acceptLoop()

	quit := make(chan os.Signal)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM) // 接受软中断信号并且传递到 channel

	<-quit

	// 通知主线程在完成任务后退出，防止有任务进行到一半
	s.quit = true
	<-s.quitFlag

	logger.Info("Server Shutdown...")
}
