package server

import (
	"MemTable/logger"
	"MemTable/resp"
	"fmt"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
)

// 放入到定时队列中运行，就可以阻塞主线程
func (s *Server) sendSyncToMaster(url string) bool {
	conn, err := net.Dial("tcp", url)
	if err != nil {
		logger.Error("Sync: Dial Failed", err.Error())
		return false
	}

	client := NewClient(conn)

	rdLen := 0
	parsedPos := 0
	rdbSize := 0
	handshakeBuff := make([]byte, 0, 65536)

	pingStr := "*1\r\n$4\r\nping\r\n"
	_, err = client.cnn.Write([]byte(pingStr))

	if err != nil {
		logger.Error("Sync: Ping Failed", err.Error())
		return false
	}

	const (
		PING = iota
		SYNC
		WAITRDB
		FINISHED
	)
	shakeStatus := PING

	for {

		rdBuff := make([]byte, 1000)

		rd, err := client.cnn.Read(rdBuff)
		handshakeBuff = append(handshakeBuff, rdBuff[0:rd]...)

		rdLen += rd
		if err != nil {
			println(err.Error())
		}

		if shakeStatus == PING {

			// 验证 pingRes 是否为 +pong\r\n
			if strings.ToLower(string(handshakeBuff[0:rdLen])) != "+pong\r\n" {
				logger.Error("PSync: Master Reply Ping Error", string(handshakeBuff[0:rdLen]))
				return false
			}

			parsedPos += rdLen

			_, err = client.cnn.Write([]byte("*1\r\n$4\r\nsync\r\n"))
			if err != nil {
				logger.Error("Sync: Write SYNC Command Failed", err.Error())
				return false
			}
			shakeStatus = SYNC

		} else if shakeStatus == SYNC {

			//for i := parsedPos; i < rdLen; i++ {
			if handshakeBuff[parsedPos] == '$' {

				for j := parsedPos + 1; j < rdLen; j++ {
					if handshakeBuff[j] == '\r' && handshakeBuff[j+1] == '\n' {
						rdbSize, _ = strconv.Atoi(string(handshakeBuff[parsedPos+1 : j]))
						parsedPos = j + 2
						shakeStatus = WAITRDB

					}
				}
			} else {

				logger.Error("Sync: Master Don't Understand Sync With Wrong Reply", string(handshakeBuff[parsedPos:rdLen]))
				return false

			}

		} else if shakeStatus == WAITRDB {
			if rdbSize > 0 && parsedPos+rdbSize <= rdLen {
				rdbFile, _ := os.Create("received.rdb")
				_, err = rdbFile.Write(handshakeBuff[parsedPos : parsedPos+rdbSize])
				if err != nil {
					logger.Error("Sync: Write RDBFile Failed", err.Error())
				}

				// 这里需要设置 repl-id 和 repl-offset 吗
				break
			}
		}

	}

	// 删除本地的 rdb 和 aof

	// 从 rdb 中恢复
	s.recoverFromRDB(path.Join(s.dir, s.aofFile), path.Join(s.dir, "received.rdb"))
	_ = os.Rename(path.Join(s.dir, "received.rdb"), path.Join(s.dir, s.rdbFile))
	// 关闭回复
	s.clis.AddClientIfNotExist(client)

	//fixme
	s.standAloneToSlave(client, s.runID, s.offset)

	go s.waitMasterNotification(client)

	return true
}

func (s *Server) SendPSyncToMaster(url string) bool {
	conn, err := net.Dial("tcp", url)
	if err != nil {
		logger.Error("PSync: Dial Failed", err.Error())
		return false
	}

	client := NewClient(conn)

	rdLen := 0
	parsedPos := 0
	rdbSize := 0
	handshakeBuff := make([]byte, 0, 65536)

	pingStr := "*1\r\n$4\r\nping\r\n"
	_, err = client.cnn.Write([]byte(pingStr))

	if err != nil {
		logger.Error("PSync: Ping Failed", err.Error())
		return false
	}

	const (
		PING = iota
		PSYNC
		FULLSYNC
		CONTINUE
		FINISHED
	)
	shakeStatus := PING

	// 应该建立检查机制
	for {

		rdBuff := make([]byte, 1000)

		rd, err := client.cnn.Read(rdBuff)
		handshakeBuff = append(handshakeBuff, rdBuff[0:rd]...)

		//rd, err := client.cnn.Read(handshakeBuff[rdLen:])
		rdLen += rd
		if err != nil {
			logger.Error("PSync: Read Failed", err.Error())
			return false
		}

		println(string(handshakeBuff[0:rdLen]))

		if shakeStatus == PING {

			// 验证 pingRes 是否为 +pong\r\n
			if strings.ToLower(string(handshakeBuff[0:rdLen])) != "+pong\r\n" {
				logger.Error("PSync: Master Reply Ping Error", string(handshakeBuff[0:rdLen]))
				return false
			}

			parsedPos += rdLen

			replID := "?"
			replOffset := -1

			if s.runID != "" {
				replID = s.runID
				replOffset = int(s.offset)
			}
			replOffsetStr := strconv.Itoa(replOffset)

			_, err = client.cnn.Write([]byte(fmt.Sprintf("*3\r\n$5\r\npsync\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
				len(replID), replID, len(replOffsetStr), replOffsetStr)))

			if err != nil {
				logger.Error("PSync: Write PSYNC Command Failed", err.Error())
				return false
			}
			shakeStatus = PSYNC

		} else if shakeStatus == PSYNC {

			//for i := parsedPos; i < rdLen; i++ {
			if handshakeBuff[parsedPos] == '+' {

				if rdLen >= parsedPos+11 && strings.ToUpper(string(handshakeBuff[parsedPos+1:parsedPos+11])) == "FULLRESYNC" {

					parsedPos = parsedPos + 11
					shakeStatus = FULLSYNC

				} else if rdLen >= parsedPos+9 && strings.ToUpper(string(handshakeBuff[parsedPos+1:parsedPos+19])) == "CONTINUE" {
					parsedPos = parsedPos + 9
					shakeStatus = CONTINUE
				} else if rdLen >= parsedPos+11 {
					logger.Error("PSync: Master Don't Understand PSync With Wrong Reply", string(handshakeBuff[parsedPos:rdLen]))
					return false
				}

			} else {
				logger.Error("PSync: Master Don't Understand PSync With Wrong Reply", string(handshakeBuff[parsedPos:rdLen]))
				return false
			}

		} else if shakeStatus == FULLSYNC {

			if parsedPos == ' ' {
				parsedPos++
			}
			s.runID = string(handshakeBuff[parsedPos : parsedPos+40])

			parsedPos += 42

			replOffsetStr := ""
			for handshakeBuff[parsedPos] != '\r' {
				replOffsetStr += string(handshakeBuff[parsedPos])
				parsedPos++
			}
			replOffset, err := strconv.Atoi(replOffsetStr)
			if err != nil {
				logger.Error("PSync: Invalid ReplOffset", replOffsetStr)
				return false
			}
			s.offset = uint64(replOffset)
			parsedPos += 2

			//for i := parsedPos; i < rdLen; i++ {
			if handshakeBuff[parsedPos] == '$' {

				for j := parsedPos + 1; j < rdLen; j++ {
					if handshakeBuff[j] == '\r' && handshakeBuff[j+1] == '\n' {
						rdbSize, _ = strconv.Atoi(string(handshakeBuff[parsedPos+1 : j]))
						parsedPos = j + 2
					}
				}

			} else {
				logger.Error("PSync: Master Don't Understand PSync With Wrong Reply", string(handshakeBuff[parsedPos:rdLen]))
				return false
			}

			if rdbSize > 0 && parsedPos+rdbSize <= rdLen {
				rdbFile, _ := os.Create("received.rdb")

				_, err = rdbFile.Write(handshakeBuff[parsedPos : parsedPos+rdbSize])

				if err != nil {
					logger.Error("PSync: Write RDBFile Failed", err.Error())
				}

				// 从 rdb 中恢复
				s.recoverFromRDB(path.Join(s.dir, s.aofFile), path.Join(s.dir, "received.rdb"))
				_ = os.Rename(path.Join(s.dir, "received.rdb"), path.Join(s.dir, s.rdbFile))

				// 这里需要设置 repl-id 和 repl-offset 吗
				break
			}

		} else if shakeStatus == CONTINUE {
			if parsedPos == ' ' {
				parsedPos++
			}
			s.runID = string(handshakeBuff[parsedPos : parsedPos+40])

			parsedPos += 42

			replOffsetStr := ""
			for handshakeBuff[parsedPos] != '\r' {
				replOffsetStr += string(handshakeBuff[parsedPos])
				parsedPos++
			}
			replOffset, err := strconv.Atoi(replOffsetStr)
			if err != nil {
				logger.Error("PSync: Invalid ReplOffset", replOffsetStr)
				return false
			}
			s.offset = uint64(replOffset)
			parsedPos += 2
		}

	}

	// 关闭回复
	s.clis.AddClientIfNotExist(client)

	//fixme
	s.standAloneToSlave(client, s.runID, s.offset)

	go s.waitMasterNotification(client)

	return true
}

func (s *Server) waitMasterNotification(client *Client) {
	logger.Info("Replica: Sync Finished with success")

	ch := resp.ParseStream(client.cnn)

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

				client.cmd = plain.ToCommand()
				client.raw = parsed.Data.ToBytes()

			} else if array, ok := parsed.Data.(*resp.ArrayData); ok {

				client.cmd = array.ToCommand()
				client.raw = parsed.Data.ToBytes()

			} else {

				logger.Warning("Client", client.id, "parse Command Error:\n", string(parsed.Data.ByteData()))
				running = false
				break
			}

			// 如果解析完毕有可以执行的命令，则发送给主线程执行
			s.commands <- client

			// 等待执行完毕并且丢弃，不回复主节点
			<-client.res

		case <-client.exit:
			running = false

		}

	}

	// 如果是读写发生错误，需要通知事件循环来关闭连接
	if client.status != EXIT {
		// 说明这是异常退出的
		logger.Error("Replication: Connection with master lost.")
		s.masterAlive = false
	}
}

func (s *Server) reconnectToMaster() {

	logger.Info("Replica: Reconnecting to Master", s.Master.cnn.RemoteAddr().String())
	if s.sendSyncToMaster(s.Master.cnn.RemoteAddr().String()) {
		s.masterAlive = true
		logger.Error("Replica: Reconnect to Master Succeeded")
	} else {
		logger.Error("Replica: Reconnect to Master Failed")
	}

}
