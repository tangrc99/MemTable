package unused

/*
import (
	"MemTable/config"
	"MemTable/db"
	"MemTable/db/cmd"
	"MemTable/db/structure"
	"MemTable/logger"
	"MemTable/resp"
	"MemTable/server"
	"fmt"
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

	database *db.DataBase  // 数据库的序号
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
		status: WAIT,
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

			array, ok := parsed.Data.(*resp.ArrayData)
			if !ok {
				println("command eeeeeeeor")
			}

			client.cmd = array.ToCommand()
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
	if client.status != EXIT {
		// 说明这是异常退出的
		client.status = ERROR
		client.cmd = nil

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

	db_ := db.NewDataBase()

	for {
		timer := time.NewTimer(time.Second)
		select {
		case <-timer.C:
			println("timer arrived")
			// 需要完成定时任务

		case cli := <-commands:
			println("event arrived")

			if cli.cmd == nil {
				continue
			}
			println(cli.cmd)

			// 底层发生异常，需要关闭客户端，或者客户端已经关闭了，那么就不处理请求了
			if cli.status == ERROR || cli.status == EXIT {
				// 释放客户端资源
				delete(UUIDSet, cli.id)

				println("remove client")
				continue
			}

			// 用于判断是否为新连接
			_, exist := UUIDSet[cli.id]
			if exist {
				println("this is an old client")
			} else {
				println("this is a new client")
				UUIDSet[cli.id] = struct{}{}
				// 变更为正常状态
				cli.status = CONNECTED
			}

			// 更新时间戳
			cli.tp = time.Now()

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

func start() {

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

func init() {
	println("main init")
}

func main() {

	s := server.NewServer(fmt.Sprintf("%s:%d", config.Conf.Host, config.Conf.Port))
	s.Start()
	return

	err := logger.Init("/Users/tangrenchu/GolandProjects/MemTable/logs", "bin.log", logger.DEBUG)
	if err != nil {
		return
	}
	events := server.NewTimeEventList()

	events.AddTimeEvent(server.NewSingleTimeEvent(func() {
		println(time.Now().Unix())
		println("this is a time event")
	}, time.Now().Add(time.Second).Unix()))

	events.AddTimeEvent(server.NewPeriodTimeEvent(func() {
		println(time.Now().Unix())

		println("this is a period time event")
	}, time.Now().Add(time.Second).Unix(), time.Second))

	time.Sleep(1 * time.Second)

	events.ExecuteManyBefore(2 * time.Second)
	//println(events.ExecuteOneIfExpire())
	//println(events.ExecuteOneIfExpire())
	//println(events.ExecuteOneIfExpire())
	println(events.Size())
	//println(events.ExecuteOneIfExpire())
	//time.Sleep(1 * time.Second)
	//println(events.ExecuteOneIfExpire())

	return

	start()

	db_ := db.NewDataBase()
	str := [][]byte{[]byte("set"), []byte("key"), []byte("value")}
	cmd.Set(db_, str)

	value, ok := db_.GetKey("key")
	if ok {
		println(value.(string))
	}

	return

	list := structure.NewList()
	list.PushBack(0)
	list.PushBack(1)
	list.PushBack(2)
	list.PushBack(3)
	list.Set(645, 4)
	//nums := list.RemoveValue(66, 5)
	//println(nums)

	//list.InsertBefore(4, -1)
	//
	//list.Set(-1, 0)
	//
	//list.InsertBefore(-2, -1)
	//
	//list.RemoveValue(3, 10)

	values, ok := list.Range(3, 100)
	if ok {
		for _, v := range values {
			println(v.(int))
		}
	}
}

*/
