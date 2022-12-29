package server

import (
	"MemTable/db"
	"MemTable/db/cmd"
	"MemTable/logger"
	"MemTable/resp"
	"net"
	"time"
)

var commands = make(chan *Client, 100)

type Server struct {
	dbs   []*db.DataBase // 多个可以用于切换的数据库
	dbNum int
	clis  *ClientList // 客户端列表

	url string
}

func NewServer(url string) *Server {
	n := 2

	d := make([]*db.DataBase, n)
	for i := 0; i < n; i++ {
		d[i] = db.NewDataBase()
	}

	return &Server{
		dbs:   d,
		dbNum: n,
		clis:  NewClientList(),
		url:   "127.0.0.1:6379",
	}
}

func handleRead(conn net.Conn) {
	//data := make([]byte, 1000)
	client := NewClient(conn)

	ch := resp.ParseStream(conn)

	// 这里会阻塞等待有数据到达
	running := true
	for running {

		select {
		// 等待是否有新消息到达
		case parsed := <-ch:
			if parsed.Err != nil {
				println(parsed.Err.Error())
				running = false
				break
			}

			array, ok := parsed.Data.(*resp.ArrayData)
			if !ok {
				println("command eeeeeeeor")
			}

			client.cmd = array.ToCommand()
			// 如果解析完毕有可以执行的命令，则发送给主线程执行
			//client.cmd = string(data[0:i])
			commands <- client

		case r := <-client.res: // fixme : 这里的分支会导致客户端消息乱序吗

			// 将主线程的返回值写入到 socket 中
			_, err := conn.Write([]byte(r))

			if err != nil {
				println("write error")
				running = false
				break
			}

		case <-client.exit:
			running = false
			break
		}
	}

	// 如果是读写发生错误，需要通知事件循环来关闭连接
	if client.status != EXIT {
		// 说明这是异常退出的
		client.status = ERROR
		client.cmd = nil

		// 通知顶层
		commands <- client
	}

	println("go exit")
	err := conn.Close()
	if err != nil {
		return
	}

}

func eventLoop() {

	db_ := db.NewDataBase()
	clis := NewClientList()
	te := NewTimeEventList()

	// 每 300 秒清理一次过期客户端
	te.AddTimeEvent(NewPeriodTimeEvent(func() {
		logger.Debug("TimeEvent: Remove Inactive Clients")
		clis.RemoveLongNotUsed(1, 300*time.Second)
	}, time.Now().Add(300*time.Second).Unix(), 300*time.Second,
	))

	for {
		timer := time.NewTimer(time.Second)
		select {
		case <-timer.C:
			logger.Debug("EventLoop: Timer trigger")
			// 需要完成定时任务
			te.ExecuteOneIfExpire()

		case cli := <-commands:
			logger.Debug("EventLoop: New Event From Client", cli.id.String())

			if cli.cmd == nil {
				continue
			}
			//println(cli.cmd)

			// 底层发生异常，需要关闭客户端，或者客户端已经关闭了，那么就不处理请求了
			if cli.status == ERROR || cli.status == EXIT {
				// 释放客户端资源
				//delete(UUIDSet,cli.id)
				logger.Debug("EventLoop: Remove Closed Client", cli.id.String())
				clis.RemoveClient(cli)
				continue
			}

			// 用于判断是否为新连接
			ok := clis.AddClientIfNotExist(cli)

			// 如果是新连接
			if ok {
				logger.Debug("EventLoop: New Client")
			}
			//_, exist := UUIDSet[cli.id]
			//
			//if exist {
			//	println("this is an old client")
			//} else {
			//	println("this is a new client")
			//	UUIDSet[cli.id] = struct{}{}
			//	// 变更为正常状态
			//	cli.status = CONNECTED
			//}

			// 更新时间戳
			//cli.tp = time.Now()
			cli.UpdateTimestamp()
			// 执行命令

			var res resp.RedisData
			if len(cli.cmd) == 2 {
				res = cmd.Get(db_, cli.cmd)
			} else {
				res = cmd.Set(db_, cli.cmd)
			}

			// fixme: 现在默认是一个空命令
			//res := resp.MakeErrorData("error: unsupported command")

			// 写入回包
			cli.res <- string(res.ToBytes()) // fixme : 这里有阻塞的风险

		}
	}

}

func backgroundLoop() {

	// 完成后台的任务
}

func Start() {

	err := logger.Init("/Users/tangrenchu/GolandProjects/MemTable/logs", "bin.log", logger.DEBUG)
	if err != nil {
		return
	}

	listener, err := net.Listen("tcp", "127.0.0.1:6379")
	if err != nil {
		return
	}

	go eventLoop()

	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		go handleRead(conn)

	}
}
