package server

import (
	"fmt"
	"github.com/tangrc99/MemTable/logger"
	"github.com/tangrc99/MemTable/server/global"
	"github.com/tangrc99/MemTable/utils/rand_str"
	"github.com/tangrc99/MemTable/utils/ring_buffer"
	"strconv"
)

const (
	StandAlone = 0
	Master     = 1
	Slave      = 2
)

type ReplicaStatus struct {
	role      int
	capacity  uint64
	offset    uint64
	rdbOffset uint64 // 生成 rdb 时的 offset
	runID     string // 集群 id
	backLog   ring_buffer.RingBuffer

	// Master 需要的
	onLineSlaves  map[*Client]struct{}
	offLineSlaves map[*Client]struct{}
	initSlaves    map[*Client]struct{}

	// Slave 需要的
	Master      *Client
	masterAlive bool
}

func (s *ReplicaStatus) updateReplicaStatus(event *Event) {

	switch s.role {

	case StandAlone:
		return

	case Master:
		s.appendBackLog(event)

	case Slave:
		if event.cli == s.Master {
			s.offset += uint64(len(event.raw))
		}
	}
}

func (s *Server) handleReplicaEvents() {
	switch s.role {
	case StandAlone:
		return
	case Master:
		s.sendBackLog()
	case Slave:

		if s.masterAlive == true {
			s.sendOffsetToMaster()
		} else if s.clusterStatus.state == ClusterOK {
			s.clusterStatus.state = ClusterDown
		} else {
			s.reconnectToMaster()
		}
	}
}

// Master 接口

func (s *ReplicaStatus) minOffset() uint64 {
	return s.backLog.LowWaterLevel()
}

func (s *ReplicaStatus) registerSlave(cli *Client) {

	if s.role == Slave {
		logger.Error("Replica: Slave Received syncToDisk Request")
		return
	}

	if s.role == StandAlone {
		// 准备
		s.standAloneToMaster()
	}

	cli.blocked = true
	s.initSlaves[cli] = struct{}{}
	cli.slaveStatus = slaveInit
	cli.offset = 0
}

func (s *ReplicaStatus) changeSlaveOnline(cli *Client, slaveOffset uint64) {

	cli.slaveStatus = slaveOnline
	cli.offset = slaveOffset
	cli.blocked = false
}

func (s *ReplicaStatus) standAloneToMaster() {
	s.role = Master
	s.runID = rand_str.RandHexString(40)
	s.backLog.Init(1 << 20)
	s.capacity = 1 << 20
	s.rdbOffset = 0
	s.offset = 0
	s.onLineSlaves = make(map[*Client]struct{})
	s.offLineSlaves = make(map[*Client]struct{})
	s.initSlaves = make(map[*Client]struct{})
}

func (s *ReplicaStatus) sendBackLog() {

	if s.role != Master {
		return
	}

	// 检查是否有正在初始化的客户端
	for cli := range s.initSlaves {
		if cli.slaveStatus == slaveOnline {
			delete(s.initSlaves, cli)
			s.onLineSlaves[cli] = struct{}{}

		}
	}

	// 选择存活 slave 发送 backlog
	for cli := range s.onLineSlaves {

		// 如果 slave 落后过多，设置为断线，停止发送 backlog
		if cli.offset < s.minOffset() {
			delete(s.onLineSlaves, cli)
			s.offLineSlaves[cli] = struct{}{}
			cli.slaveStatus = slaveOffline
		}

		// 更新时间戳

		// 一次最多读取 1kb
		bytes := s.backLog.Read(cli.offset, 1<<10)

		if len(bytes) == 0 {

			//从服务器完成同步，发送 ping 命令
			_, _ = cli.cnn.Write([]byte("*1\r\n$4\r\nping\r\n"))

		} else {

			n, err := cli.cnn.Write(bytes)

			cli.offset += uint64(n)
			if err != nil {
				// 如果发生错误，等待 slave 的offset 落后会自动转换为 offline
				continue
			}

		}
		// 成功写入会更新时间戳
		cli.UpdateTimestamp(global.Now)

	}

}

func (s *ReplicaStatus) appendBackLog(event *Event) {
	if s.role != Master || len(event.raw) <= 0 {
		return
	}
	// 只有写命令需要持久化

	// 多数据库场景需要加入数据库选择语句
	dbStr := strconv.Itoa(event.cli.dbSeq)
	s.offset = s.backLog.Append([]byte(fmt.Sprintf("*2\r\n$6\r\nselect\r\n$%d\r\n%s\r\n", len(dbStr), dbStr)))

	s.offset = s.backLog.Append(event.raw)

}

func (s *Server) rdbForReplica() uint64 {

	// 不需要进行复制
	if s.rdbOffset > 0 && s.offset-s.rdbOffset < s.capacity {
		return s.rdbOffset
	}

	ok := s.BGRDB()

	// 如果正在复制，需要等待
	if !ok {
		s.waitForRDBFinished()
	}

	s.rdbOffset = s.offset

	return s.rdbOffset

}

// Slaves 接口

func (s *ReplicaStatus) standAloneToSlave(client *Client, runId string, offset uint64) {
	s.masterAlive = true
	s.Master = client
	s.role = Slave
	s.runID = runId
	s.offset = offset
}

func (s *ReplicaStatus) sendOffsetToMaster() {

	if s.role != Slave || s.Master == nil {
		logger.Error("Replica Not Slave Node Try Runs sendOffsetToMaster")
	}

	offsetStr := strconv.Itoa(int(s.offset))
	replconfCMD := fmt.Sprintf("*3\r\n$8\r\nreplconf\r\n$3\r\nack\r\n$%d\r\n%s\r\n", len(offsetStr), offsetStr)

	_, _ = s.Master.cnn.Write([]byte(replconfCMD))
}

func (s *ReplicaStatus) slaveToStandAlone() {
	s.role = StandAlone
	s.masterAlive = false
	_ = s.Master.cnn.Close()
	s.Master = nil
}

const (
	slaveNot = iota
	slaveInit
	slaveOnline
	slaveOffline
)

type SlaveStatus struct {
	slaveStatus int
	offset      uint64
}
