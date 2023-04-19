package server

import (
	"fmt"
	"github.com/tangrc99/MemTable/config"
	"github.com/tangrc99/MemTable/server/global"
	"github.com/tangrc99/MemTable/utils/sys_status"
	"os"
	"strings"
	"time"
)

type Status struct {

	// Server
	pid          int
	host         string
	tcpPort      int
	tlsPort      int
	time         time.Time
	startTime    time.Time
	startTimeDay time.Time

	// Clients
	connectedClients int
	maxClients       int

	// Memory
	usedMemory      int64
	usedMemoryHuman float64
	maxMemory       uint64

	// Replication
	role            string
	connectedSlaves int
	backlogSize     uint64
	//backlogOffset   int

	// Keyspace

	sys_status.SysStatus
}

func NewStatus() *Status {

	s := &Status{

		pid:       os.Getpid(),
		host:      config.Conf.Host,
		tcpPort:   config.Conf.Port,
		tlsPort:   config.Conf.TLSPort,
		time:      global.Now,
		startTime: time.Now(),

		maxClients: config.Conf.MaxClients,
		maxMemory:  config.Conf.MaxMemory,
	}

	s.UpdateSysStatus()

	return s
}

func (s *Server) UpdateStatus() {
	sts := s.sts

	sts.time = global.Now
	sts.connectedClients = s.clis.Size()
	sts.usedMemory = s.cost
	sts.usedMemoryHuman = float64(s.cost / 1024 / 1024)

	sts.connectedSlaves = len(s.onLineSlaves)
	sts.backlogSize = s.backLog.HighWaterLevel()

	sts.UpdateSysStatus()
}

func (s *Server) Information(section string) string {

	section = strings.ToLower(section)

	b := strings.Builder{}

	if section == "" || section == "server" {

		if b.Len() > 0 {
			b.WriteString("\n")
		}
		b.WriteString("# Server\n")
		b.WriteString(fmt.Sprintf("pid:%d\n", s.sts.pid))
		b.WriteString(fmt.Sprintf("host:%s\n", s.sts.host))
		b.WriteString(fmt.Sprintf("tcp_port:%d\n", s.sts.tcpPort))
		b.WriteString(fmt.Sprintf("tls_port:%d\n", s.sts.tlsPort))
		b.WriteString(fmt.Sprintf("server_time_us:%d\n", s.sts.time.UnixMicro()))
		b.WriteString(fmt.Sprintf("start_time:%d\n", s.sts.time.Unix()-s.sts.startTime.Unix()))
		b.WriteString(fmt.Sprintf("start_time_day:%d\n", (s.sts.time.Unix()-s.sts.startTime.Unix())/86400))

	}

	if section == "" || section == "clients" {

		if b.Len() > 0 {
			b.WriteString("\n")
		}
		b.WriteString("# Clients\n")
		b.WriteString(fmt.Sprintf("connected_clients:%d\n", s.sts.connectedClients))
		b.WriteString(fmt.Sprintf("max_clients:%d\n", s.sts.maxClients))
	}

	if section == "" || section == "memory" {

		if b.Len() > 0 {
			b.WriteString("\n")
		}
		b.WriteString("# Memory\n")
		b.WriteString(fmt.Sprintf("used_memory:%d\n", s.sts.usedMemory))
		b.WriteString(fmt.Sprintf("used_memory_human:%.2fM\n", s.sts.usedMemoryHuman))
		b.WriteString(fmt.Sprintf("max_memory:%d\n", s.sts.maxMemory))
		b.WriteString(fmt.Sprintf("used_memory_percent:%.2f%%\n", float64(s.sts.usedMemory)/float64(s.sts.maxMemory)))

	}

	if section == "" || section == "system" {

		if b.Len() > 0 {
			b.WriteString("\n")
		}
		b.WriteString("# System\n")
		b.WriteString(fmt.Sprintf("used_cpu_sys:%.4f%%\n", s.sts.CPUPercents))
		b.WriteString(fmt.Sprintf("total_memory:%d\n", s.sts.Total))
		b.WriteString(fmt.Sprintf("total_used:%d\n", s.sts.MemUsed))
		b.WriteString(fmt.Sprintf("total_free:%d\n", s.sts.MemFree))
		b.WriteString(fmt.Sprintf("used_percent:%.2f%%\n", float64(s.sts.MemUsed)/float64(s.sts.Total)))

	}

	return b.String()
}
