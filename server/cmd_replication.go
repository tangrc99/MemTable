package server

import (
	"github.com/tangrc99/MemTable/logger"
	"github.com/tangrc99/MemTable/resp"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

func syncCMD(server *Server, cli *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "sync", 1)
	if !ok {
		return e
	}

	server.registerSlave(cli)

	// 后台执行 bgsave 后将文件发送给对方

	go func() {

		offset := server.rdbForReplica()

		server.waitForRDBFinished()

		rdbFile, err := os.Open("dump.rdb")
		if err != nil {
			logger.Error("syncToDisk: No RDBFile:", err.Error())
			return
		}

		info, _ := rdbFile.Stat()
		info.Size()
		rdbHeader := "$" + strconv.FormatInt(info.Size(), 10) + resp.CRLF

		_, err = cli.cnn.Write([]byte(rdbHeader))
		if err != nil {
			logger.Error("syncToDisk: Send RDBHead Failed:", err.Error())
			return
		}

		_, err = io.Copy(cli.cnn, rdbFile)
		if err != nil {
			logger.Error("syncToDisk: Send RDBFile Failed:", err.Error())
			return
		}

		server.changeSlaveOnline(cli, offset)

		// Cluster 初始化阶段会自己建立客户端，不使用这里的连接
		if server.state == ClusterOK {
			server.upNodeAnnounce(cli.cnn.RemoteAddr().String())
		}
	}()

	return resp.MakeEmptyArrayData()
}

func psync(server *Server, cli *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "psync", 3)
	if !ok {
		return e
	}

	replID := string(cmd[1])
	replOffset, err := strconv.ParseInt(string(cmd[2]), 10, 64)
	if err != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}

	server.registerSlave(cli)

	// 检查对方的序列号以及 replOffset
	if replID != server.runID || replOffset < int64(server.minOffset()) {

		go func() {

			offset := server.rdbForReplica()
			server.waitForRDBFinished()

			rdbFile, err := os.Open(path.Join(server.dir, server.rdbFile))
			if err != nil {
				logger.Error("syncToDisk: No RDBFile:", err.Error())
				return
			}

			header := "+FULLRESYNC " + server.runID + " " +
				strconv.FormatInt(int64(offset), 10) + "\r\n"

			_, err = cli.cnn.Write([]byte(header))
			if err != nil {
				logger.Error("syncToDisk: Send Header Failed:", err.Error())
				return
			}

			info, _ := rdbFile.Stat()
			info.Size()
			rdbHeader := "$" + strconv.FormatInt(info.Size(), 10) + resp.CRLF

			_, err = cli.cnn.Write([]byte(rdbHeader))
			if err != nil {
				logger.Error("syncToDisk: Send RDBHead Failed:", err.Error())
				return
			}

			_, err = io.Copy(cli.cnn, rdbFile)
			if err != nil {
				logger.Error("syncToDisk: Send RDBFile Failed:", err.Error())
				return
			}

			server.changeSlaveOnline(cli, offset)

		}()

	} else {

		header := "+CONTINUE " + server.runID + resp.CRLF
		_, err = cli.cnn.Write([]byte(header))
		if err != nil {
			logger.Error("syncToDisk: Send Header Failed:", err.Error())
			return resp.MakeEmptyArrayData()

		}
		// 增量 sync
		server.changeSlaveOnline(cli, uint64(replOffset))

		// Cluster 初始化阶段会自己建立客户端，不使用这里的连接
		if server.state == ClusterOK {
			server.upNodeAnnounce(cli.cnn.RemoteAddr().String())
		}

	}

	return resp.MakeEmptyArrayData()

}

func replconf(_ *Server, cli *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "replconf", 2)
	if !ok {
		return e
	}

	if len(cmd) >= 2 {
		key := strings.ToLower(string(cmd[1]))
		value := string(cmd[2])
		switch key {
		case "ack":

			offset, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return resp.MakeErrorData("ERR value is not an integer or out of range")
			}
			cli.offset = uint64(offset)
			return resp.MakePlainData("")
		}
	}
	return resp.MakeStringData("OK")
}

func slaveof(server *Server, _ *Client, cmd [][]byte) resp.RedisData {
	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "slaveof", 3)
	if !ok {
		return e
	}

	if strings.ToLower(string(cmd[1])) == "no" && strings.ToLower(string(cmd[2])) == "one" {
		// slaveof no one

		server.slaveToStandAlone()
		return resp.MakeStringData("OK")
	}

	// 检查地址并且连接，如果连接完成

	// 创建一个客户端并且连接到对方
	url := string(cmd[1]) + ":" + string(cmd[2])

	server.tl.AddTimeEvent(NewSingleTimeEvent(func() {

		ok := server.sendSyncToMaster(url)
		if !ok {
			logger.Error("syncToDisk: Failed")
		}

	}, time.Now().Unix()))

	return resp.MakeStringData("OK")
}

func RegisterReplicationCommands() {
	RegisterCommand("sync", syncCMD, RD)
	RegisterCommand("psync", psync, RD)
	RegisterCommand("replconf", replconf, RD)

	RegisterCommand("slaveof", slaveof, RD)
}
