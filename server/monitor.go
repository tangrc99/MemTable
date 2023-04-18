package server

import (
	"fmt"
	"github.com/tangrc99/MemTable/db/structure"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/global"
	"strings"
)

// Monitor 是服务器的监视器，所有监视器客户端都在该结构体中维护。监视器消息的通知是异步的，对性能影响比较小。
type Monitor struct {
	commands chan *resp.StringData
	monitors *structure.List
	rookie   *Client
}

func NewMonitor() *Monitor {

	m := &Monitor{
		commands: make(chan *resp.StringData, 30),
		monitors: structure.NewList(),
		rookie:   nil,
	}

	go func() {
		for msg := range m.commands {
			for n := m.monitors.FrontNode(); n != nil; n = n.Next() {
				cli := n.Value.(*Client)
				content := msg.ToBytes()
				for wr := 0; wr < len(content); {
					nn, err := cli.cnn.Write(msg.ToBytes())
					if err != nil {
						break
					}
					wr += nn
				}
			}
		}
	}()

	return m
}

func (m *Monitor) AddMonitor(cli *Client) {
	m.rookie = cli
}

func (m *Monitor) RemoveMonitor(cli *Client) {
	m.monitors.Remove(cli)
}

// NotifyAll 将一个通知时间发送给所有的监视器客户端
func (m *Monitor) NotifyAll(event *Event) {

	// 没有监视器，不需要进行通知
	if !m.monitors.Empty() {

		b := strings.Builder{}

		if event.cli.cnn == nil {
			b.WriteString(fmt.Sprintf("%d [%d %s]", global.Now.Unix(), event.cli.dbSeq, "none"))

		} else {

			b.WriteString(fmt.Sprintf("%d [%d %s]", global.Now.Unix(), event.cli.dbSeq, event.cli.cnn.RemoteAddr().String()))
		}

		for _, cmd := range event.cmd {
			b.WriteString(fmt.Sprintf(" \"%s\"", cmd))
		}

		// 这里不会死锁，但是有可能会陷入阻塞。因为发送的异步的，如果命令执行快于发送消息，就有可能造成阻塞。
		m.commands <- resp.MakeStringData(b.String())

	}

	// 将新注册的监视器放入到
	if m.rookie != nil {
		m.monitors.PushBack(m.rookie)
		m.rookie = nil
	}
}

// Stop 关闭监视器，关闭后不能再次重新打开
func (m *Monitor) Stop() {
	close(m.commands)
}
