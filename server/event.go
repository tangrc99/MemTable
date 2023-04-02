package server

import (
	"sync"
)

func init() {
	ePool = eventPool{
		pool: sync.Pool{
			New: func() any { return &Event{cli: nil} },
		},
	}
}

// eventPool 是对 sync.Pool 的封装，用于获取 Event 对象
type eventPool struct {
	pool sync.Pool
}

var ePool eventPool

func (p *eventPool) newEvent(cli *Client) *Event {
	e := p.pool.Get().(*Event)
	e.cli = cli
	e.raw = cli.raw
	e.cmd = cli.cmd
	e.pipelined = cli.pipelined
	return e
}

func (p *eventPool) putEvent(e *Event) {
	e.cli = nil
	p.pool.Put(e)
}

// Event 是一个客户端命令
type Event struct {
	cmd       [][]byte // 经过解析后的命令
	raw       []byte   // 当前命令的 resp 格式
	cli       *Client  // 命令所属客户端
	pipelined bool     // 是否使用了 pipeline 格式
}
