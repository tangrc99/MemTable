package main

import (
	"MemTable/db/structure"
	"MemTable/resp"
	"github.com/gofrs/uuid"
	"net"
	"time"
)

type Client struct {
	cmd []byte    // 当前命令
	cnn net.Conn  // 连接实例
	id  uuid.UUID // Cli 编号
	tp  time.Time // 通信时间戳

	status int // 状态 0 等待连接 1 正常 -1 退出 -2 异常

	database int           // 数据库的序号
	exit     chan struct{} // 退出标志
	res      chan string   // 回包
}

var commands = make(chan *Client, 100)

func handleRead(conn net.Conn) {
	//data := make([]byte, 1000)
	uid := uuid.Must(uuid.NewV1())
	client := Client{
		cnn:    conn,
		id:     uid,
		status: 0,
		exit:   make(chan struct{}, 1),
		res:    make(chan string, 10),
	}

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
			client.cmd = parsed.Data.ByteData()
			// 如果解析完毕有可以执行的命令，则发送给主线程执行
			//client.cmd = string(data[0:i])
			commands <- &client

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
	if client.status != -1 {
		// 说明这是异常退出的
		client.status = -2
		client.cmd = []byte("")

		// 通知顶层
		commands <- &client
	}

	println("go exit")
	err := conn.Close()
	if err != nil {
		return
	}

}

var UUIDSet = make(map[uuid.UUID]struct{}) // 用于判断是否为新链接

func eventLoop() {

	for {
		timer := time.NewTimer(time.Second)
		select {
		case <-timer.C:
			println("timer arrived")
			// 需要完成定时任务

		case cmd := <-commands:
			println("event arrived")
			println(cmd.cmd)

			// 底层发生异常，需要关闭客户端，或者客户端已经关闭了，那么就不处理请求了
			if cmd.status == -2 || cmd.status == -1 {
				// 释放客户端资源
				delete(UUIDSet, cmd.id)

				println("remove client")
				continue
			}

			// 用于判断是否为新连接
			_, exist := UUIDSet[cmd.id]
			if exist {
				println("this is an old client")
			} else {
				println("this is a new client")
				UUIDSet[cmd.id] = struct{}{}
				// 变更为正常状态
				cmd.status = 1
			}

			// 更新时间戳
			cmd.tp = time.Now()

			// 执行命令

			// fixme: 现在默认是一个空命令
			res := resp.MakeErrorData("error: unsupported command")

			// 写入回包
			cmd.res <- string(res.ToBytes()) // fixme : 这里有阻塞的风险

		}
	}

}

func backgroundLoop() {

	// 完成后台的任务
}

func start() {
	listener, err := net.Listen("tcp", "127.0.0.1:8888")
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

func main() {
	skipList := structure.NewSkipList(3)
	skipList.Insert("1", 1)
	skipList.InsertIfNotExist("2", "5")
	skipList.Insert("3", 1)

	skipList.Delete("2")

	v, ok := skipList.Get("3")
	if ok {
		println(v)
	} else {
		println("not found")
	}

}
