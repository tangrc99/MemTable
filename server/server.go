package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/tangrc99/MemTable/config"
	"github.com/tangrc99/MemTable/db"
	"github.com/tangrc99/MemTable/logger"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/acl"
	"github.com/tangrc99/MemTable/server/global"
	"github.com/tangrc99/MemTable/utils/gopool"
	"net"
	"os"
	"os/signal"
	"path"
	"regexp"
	"sync"
	"syscall"
	"time"
)

type Server struct {
	url         string       // 监听 url
	tlsUrl      string       // tls url
	uds         string       // unix domain socket
	listener    net.Listener // listener
	tlsListener net.Listener // tls listener
	uListener   net.Listener // uds listener
	dir         string       // 工作目录

	// 数据库部分
	dbs          []*db.DataBase // 多个可以用于切换的数据库
	Chs          *db.Channels   // 订阅发布频道
	dbNum        int            //数据库数量
	evictChannel []chan string

	// 客户端部分
	clis       *ClientList // 客户端列表
	cliTimeout int         // 客户端失效时间
	maxClients int         // 最大客户端数量
	events     chan *Event // 用于解析完毕的协程同步

	tl *TimeEventList // 时间事件链表

	// 退出控制
	quit     bool
	quitFlag chan struct{}

	// 持久化
	rdbFile    string     // rdb 文件名
	dirty      int        // 脏数据计数器
	checkPoint int64      // rdb 时间
	RDBStatus             // rdb 文件状态
	aofFile    string     // aof 文件名
	aof        *aofBuffer // aof 缓冲区
	aofEnabled bool       // 是否开启 aof

	full bool // 表示已经写满
	cost int64

	// 慢查询日志
	slowlog *slowLog
	// 监视器
	monitors *Monitor

	// 协程池
	gopool *gopool.Pool // 用于客户端启动的协程池
	sts    *Status

	// 主从复制
	ReplicaStatus

	// 集群
	clusterStatus

	msgPool sync.Pool

	acl *acl.ACL
}

func NewServer() *Server {
	// 配置数据库
	d := make([]*db.DataBase, config.Conf.DataBases)

	for i := 0; i < config.Conf.DataBases; i++ {
		switch config.Conf.Eviction {
		case "no":
			d[i] = db.NewDataBase(slotNum, db.WithEviction(db.NoEviction))
		case "lru":
			d[i] = db.NewDataBase(slotNum, db.WithEviction(db.EvictLRU))
		case "lfu":
			d[i] = db.NewDataBase(slotNum, db.WithEviction(db.EvictLFU))

		}
	}

	s := &Server{
		dbs:        d,
		dbNum:      config.Conf.DataBases,
		Chs:        db.NewChannels(),
		clis:       NewClientList(),
		tl:         NewTimeEventList(),
		events:     make(chan *Event, 10000),
		quit:       false,
		quitFlag:   make(chan struct{}),
		rdbFile:    config.Conf.RDBFile,
		dirty:      0,
		sts:        NewStatus(),
		cliTimeout: config.Conf.Timeout,
		maxClients: config.Conf.MaxClients,
		dir:        config.Conf.Dir,
		aofEnabled: config.Conf.AppendOnly,
		aofFile:    "appendonly.aof",
		slowlog:    newSlowLog(config.Conf.SlowLogMaxLen),
		monitors:   NewMonitor(),
		acl:        acl.NewAccessControlList(config.Conf.ACLFile),
	}

	// check the port
	if config.Conf.Port != 0 {
		s.url = fmt.Sprintf("%s:%d", config.Conf.Host, config.Conf.Port)
	}
	if config.Conf.TLSPort != 0 {
		s.tlsUrl = fmt.Sprintf("%s:%d", config.Conf.Host, config.Conf.TLSPort)
	}

	evictChannel := make([]chan string, s.dbNum)
	for i := range evictChannel {
		evictChannel[i] = make(chan string, 100)
	}
	s.evictChannel = evictChannel

	// 逐出以及过期是通过 del 的方式写入日志的
	if s.aofEnabled {
		s.StartEvictionNotification()
	}

	env = initLuaEnv(s)

	if config.Conf.ClusterEnable {
		s.initCluster(s)
	}

	return s
}

func (s *Server) InitModules() {
	// aof 开关
	if config.Conf.AppendOnly {
		logger.Debug("Config: AppendOnly Enabled")
		s.aof = newAOFBuffer(config.Conf.Dir + "appendonly.aof")
	}

	if config.Conf.GoPool {
		s.gopool = gopool.NewPool(config.Conf.GoPoolSize, 0, config.Conf.GoPoolSpawn)
		logger.Debug("Config: GoPool Enabled")
	}
}

func (s *Server) handleRead(conn net.Conn) {

	client := NewClient(conn)

	logger.Info("New Client", conn.RemoteAddr().String())

	// 这里会阻塞等待有数据到达
	running := true

	req := make(chan *resp.ParsedRes, 10)

	ok := s.runInNewGoroutine(func() {
		for running && !s.quit {
			r := client.ParseStream()
			req <- r
			if r.Abort == true {
				break
			}
		}
	})
	if !ok {
		_ = conn.Close()
		return
	}

	for running && !s.quit {

		select {
		case parsed := <-req:

			if parsed.Err != nil {

				e := parsed.Err.Error()

				if e == "AGAIN" {
					continue
				} else if e == "EOF" {
					logger.Debugf("Client %s ShutDown Connection", client.cnn.RemoteAddr().String())

				} else {

					logger.Info("Client Read Error:", e)
					matched, _ := regexp.MatchString("Protocol error*", e)
					if matched {
						continue
					}

				}
				running = false
				break
			}

			// 如果无错误且消息为空，不做处理
			if parsed.Data == nil {
				continue
			}

			if plain, ok := parsed.Data.(*resp.PlainData); ok {

				client.pipelined = true
				client.cmd = plain.ToCommand()
				client.raw = parsed.Data.ToBytes()

			} else if array, ok := parsed.Data.(*resp.ArrayData); ok {

				client.cmd = array.ToCommand()
				client.raw = parsed.Data.ToBytes()

			} else {
				logger.Warning("Client parse Command Error,raw:", string(parsed.Data.ByteData()))
				running = false
				break
			}

			// 如果解析完毕有可以执行的命令，则发送给主线程执行
			s.events <- ePool.newEvent(client)

			client.pipelined = false

		// 使用 select 防止协程无法释放
		case r := <-client.res:

			// 将主线程的返回值写入到 socket 中
			_, err := conn.Write((*r).ToBytes())

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
		event := ePool.newEvent(client)

		// 通知顶层
		s.events <- event
	}

	// 防止客户端中有剩余数据未发送
sendFinish:
	for {
		select {
		case r := <-client.res:

			// 将主线程的返回值写入到 socket 中
			_, err := conn.Write((*r).ToBytes())

			if err != nil {
				logger.Warning("Client", client.id, "Write Error")
				break sendFinish
			}
		default:
			break sendFinish
		}

	}

	_ = conn.Close()

	logger.Info("Client Shutdown", conn.RemoteAddr().String())

}

func (s *Server) eventLoop() {

	s.initTimeEvents()
	timer := time.NewTimer(100 * time.Millisecond)

	for !s.quit {

		// 每一次循环都更新一次全局时钟
		global.UpdateGlobalClock()

		select {

		case <-timer.C:

			timer.Reset(100 * time.Millisecond)
			// 需要完成定时任务，这里是非阻塞的，可以使用全局时钟
			s.tl.ExecuteManyDuring(global.Now, 25*time.Millisecond)

		case event := <-s.events:

			global.UpdateGlobalClock()
			startTs := global.Now

			cli := event.cli
			logger.Debug("EventLoop: New Event From Client", cli.id.String())

			// todo:  关闭使用定时队列来实现
			// 底层发生异常，需要关闭客户端，或者客户端已经关闭了，那么就不处理请求了
			if cli.status == ERROR || cli.status == EXIT {
				// 释放客户端资源
				s.shutdownClient(cli)
				continue
			}

			// 用于判断是否为新连接
			if s.clis.AddClientIfNotExist(cli) {
				logger.Debug("EventLoop: New Client", cli.id.String())
			}

			// 更新时间戳
			cli.UpdateTimestamp(global.Now)

			// monitor
			s.monitors.NotifyAll(event)

			// 执行命令
			res, isWriteCommand := ExecCommand(s, cli, event.cmd, event.raw)

			global.UpdateGlobalClock()
			endTs := global.Now

			// slow log
			if config.Conf.SlowLogSlowerThan >= 0 {
				// this is a slow command
				if d := endTs.Sub(startTs).Microseconds(); d >= config.Conf.SlowLogSlowerThan {
					s.slowlog.appendEntry(event.cmd, d)
				}
			}

			if res == nil {
				continue
			}

			// 只有写命令需要完成aof持久化
			if isWriteCommand && fmt.Sprintf("%T", res) != "*resp.ErrorData" {

				if event.pipelined {
					event.raw = resp.PlainDataToResp(event.cmd).ToBytes()
				}

				s.appendAOF(event)
				s.updateReplicaStatus(event)
				s.dirty++
			}

			// 非阻塞状态的客户端写入回包
			if !cli.blocked {
				cli.res <- &res
			}

			// 归还
			ePool.putEvent(event)

		default:

			// TODO: 选择一些轻量级任务来做

			//if s.aofEnabled {
			//	s.aof.flush()
			//}
		}
		s.handleEvictionNotification()

	}

	// 处理退出逻辑
	logger.Info("Server: Ready To Shutdown")

	// 关闭监听
	if s.listener != nil {
		_ = s.listener.Close()
	}
	if s.tlsListener != nil {
		_ = s.tlsListener.Close()
	}
	if s.uListener != nil {
		_ = s.uListener.Close()
	}

	// 进行数据持久化
	s.saveData()

	// 关闭所有的客户端协程
	for s.clis.Size() != 0 {
		front := s.clis.list.FrontNode()
		s.clis.removeClientWithPosition(front.Value.(*Client), front)
		// 不用删除订阅
	}

	// 通知
	s.quitFlag <- struct{}{}
}

// acceptLoop 运行 Acceptor
func (s *Server) acceptLoop(listener net.Listener) {

	for !s.quit {
		conn, err := listener.Accept()
		if err != nil {
			break
		}

		// 如果客户端数量过多，尝试清理
		if s.maxClients > 0 && s.clis.Size() >= s.maxClients {
			_ = conn.Close()
		}

		if ok := s.runInNewGoroutine(func() {
			s.handleRead(conn)
		}); !ok {
			_ = conn.Close()
		}

	}
	logger.Infof("Server: Shutdown on %s", listener.Addr().String())

}

// shutdownClient 会完成一个客户端关闭后的善后工作
func (s *Server) shutdownClient(cli *Client) {
	// 释放客户端资源
	logger.Debug("EventLoop: Remove Closed Client", cli.id.String())
	cli.UnSubscribeAll(s.Chs)
	s.clis.RemoveClient(cli)
	if cli.monitored {
		s.monitors.RemoveMonitor(cli)
	}
}

func (s *Server) initTimeEvents() {

	// 每 300 秒清理一次过期客户端
	s.tl.AddTimeEvent(NewPeriodTimeEvent(func() {
		logger.Debug("TimeEvent: Remove Inactive Clients")

		// 如果设置过期值小于 0 则不需要进行清理
		if s.cliTimeout < 0 {
			return
		}

		s.clis.RemoveLongNotUsed(3, 20, time.Duration(s.cliTimeout)*time.Second)

	}, time.Now().Add(global.TECleanClients).Unix(), global.TECleanClients,
	))

	// 过期 key 清理
	s.tl.AddTimeEvent(NewPeriodTimeEvent(func() {
		logger.Debug("TimeEvent: Remove Expired Keys")

		for _, dataBase := range s.dbs {
			// 抽样 20 个，如果有 5 个过期，则再次删除
			for dataBase.CleanExpiredKeys(20) >= 5 {
			}
		}

	}, time.Now().Add(global.TEExpireKey).Unix(), global.TEExpireKey,
	))

	// AOF 刷盘
	s.tl.AddTimeEvent(NewPeriodTimeEvent(func() {
		logger.Debug("TimeEvent: AOF FLUSH")

		if s.aofEnabled {
			s.aof.flush()
		}
		//s.aof.syncToDisk()

	}, time.Now().Add(global.TEAOF).Unix(), global.TEAOF,
	))

	// bgsave 持久化 trigger
	s.tl.AddTimeEvent(NewPeriodTimeEvent(func() {
		logger.Debug("TimeEvent: RDB Check")

		if !s.aofEnabled && (s.dirty > 100 || global.Now.Unix()-s.checkPoint > 10) {
			s.BGRDB()
		}

	}, time.Now().Add(global.TEBgSave).Unix(), global.TEBgSave,
	))

	// 更新服务端信息
	s.tl.AddTimeEvent(NewPeriodTimeEvent(func() {
		logger.Debug("TimeEvent: Update Status")

		s.sts.UpdateStatus()

	}, time.Now().Add(global.TEUpdateStatus).Unix(), global.TEUpdateStatus,
	))

	// 主从复制相关操作
	s.tl.AddTimeEvent(NewPeriodTimeEvent(func() {
		logger.Debug("TimeEvent: Replication")

		s.handleReplicaEvents()

	}, time.Now().Add(global.TEReplica).Unix(), global.TEReplica,
	))

	// cluster 相关操作
	s.tl.AddTimeEvent(NewPeriodTimeEvent(func() {
		logger.Debug("TimeEvent: Cluster")

		s.handleClusterEvents()

	}, time.Now().Add(global.TECluster).Unix(), global.TECluster,
	))
}

func (s *Server) Start() {

	// 开启事务线程
	go s.eventLoop()

	var err error

	// start network server
	if s.url != "" {

		s.listener, err = net.Listen("tcp", s.url)
		if err != nil {
			logger.Error("Server:", err.Error())
			return
		}

		logger.Info("Server: Listen at", s.url)
		go s.acceptLoop(s.listener)
	}

	// start tls server
	if s.tlsUrl != "" {

		// 载入服务端证书和私钥
		srvCert, err := tls.LoadX509KeyPair(config.Conf.CertFile, config.Conf.KeyFile)
		if err != nil {
			logger.Panicf(err.Error())
		}

		// 载入根证书，用于客户端验证
		caCertPool := x509.NewCertPool()
		caCert, err := os.ReadFile(config.Conf.CaCertFile)
		if err != nil {
			logger.Panicf(err.Error())
		}
		if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
			logger.Panicf("Parse cert error, file: %s", config.Conf.CaCertFile)
		}
		tlsCfg := &tls.Config{
			InsecureSkipVerify: false,
			ClientAuth:         tls.RequestClientCert,
			ClientCAs:          caCertPool,
			Certificates:       []tls.Certificate{srvCert},
		}
		if config.Conf.AuthClient {
			tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		}

		s.tlsListener, err = tls.Listen("tcp", s.tlsUrl, tlsCfg)

		if err != nil {
			logger.Error("TLS Server:", err.Error())
		}

		logger.Info("TLS Server: Listen at", s.tlsUrl)
		go s.acceptLoop(s.tlsListener)
	}

	// start unix domain server
	if s.uds != "" {
		s.uListener, err = net.Listen("unix", s.uds)
		if err != nil {
			logger.Error("UDS Server:", err.Error())
			return
		}

		logger.Info("UDS Server: Listen at", s.url)
		go s.acceptLoop(s.uListener)
	}

	quit := make(chan os.Signal)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM) // 接受软中断信号并且传递到 channel

	<-quit

	// 通知主线程在完成任务后退出，防止有任务进行到一半
	s.quit = true
	<-s.quitFlag

	logger.Info("Server Shutdown...")
}

func (s *Server) TryRecover() {

	aof := path.Join(s.dir, s.aofFile)
	rdb := path.Join(s.dir, s.rdbFile)

	if _, err := os.Stat(aof); err == nil {
		logger.Info("Recover From AppendOnly File")
		s.recoverFromAOF(aof)
	} else if _, err := os.Stat(rdb); err == nil {
		logger.Info("Recover From RDB File")
		s.recoverFromRDB(aof, rdb)
	}

}

func (s *Server) saveData() {

	// 优先使用 aof 进行存储
	if s.aofEnabled && s.aof != nil {

		s.aof.quit()

	} else {

		ok := s.RDB(path.Join(s.dir, s.rdbFile))
		if !ok {
			logger.Error("quit: Generate RDB File Failed")
		} else {
			logger.Info("quit: Generated RDB File")
		}
	}
}

func (s *Server) collectCost() {

	s.full = false
	s.cost = s.clis.Cost()
	s.cost += s.slowlog.Cost()
	s.cost += global.RsBackLogCap
	for _, d := range s.dbs {
		s.cost += d.Cost()
	}
	if uint64(s.cost) > config.Conf.MaxMemory {
		s.full = true
	}
}

// handleReadWithoutGoroutine  不使用额外协程进行解析，在性能较差的机器上会表现较好
func (s *Server) handleReadWithoutGoroutine(conn net.Conn) {

	client := NewClient(conn)

	// 这里会阻塞等待有数据到达
	running := true

	for running && !s.quit {

		parsed := client.ParseStream()

		if parsed.Err != nil {

			e := parsed.Err.Error()
			if e == "AGAIN" {
				continue
			} else if e == "EOF" {
				logger.Debug("Client", client.id, "Peer ShutDown Connection")
			} else {
				logger.Error("Client", client.id, "Read Error:", e)
			}
			running = false
			break
		}

		if plain, ok := parsed.Data.(*resp.PlainData); ok {

			client.pipelined = true
			client.cmd = plain.ToCommand()
			client.raw = parsed.Data.ToBytes()

		} else if array, ok := parsed.Data.(*resp.ArrayData); ok {

			client.cmd = array.ToCommand()
			client.raw = parsed.Data.ToBytes()

		} else {

			fmt.Printf(string(parsed.Data.ByteData()))

			logger.Warning("Client", client.id, "parse Command Error")
			running = false
			break
		}

		// 如果解析完毕有可以执行的命令，则发送给主线程执行
		s.events <- ePool.newEvent(client)

		// 使用 select 防止协程无法释放

		r := <-client.res

		// 将主线程的返回值写入到 socket 中
		_, err := conn.Write((*r).ToBytes())

		if err != nil {
			logger.Warning("Client", client.id, "Write Error")
			running = false
			break
		}

		client.pipelined = false

	}

	// 如果是读写发生错误，需要通知事件循环来关闭连接
	if client.status != EXIT {
		// 说明这是异常退出的
		client.status = ERROR
		client.cmd = nil

		// 通知顶层
		s.events <- ePool.newEvent(client)
	}

	err := conn.Close()
	if err != nil {
		return
	}

	logger.Info("Client Shutdown", conn.RemoteAddr().String())

}
