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
	p.pool.Put(e)
}

// Event 是一个客户端命令
type Event struct {
	cmd       [][]byte // 经过解析后的命令
	raw       []byte   // 当前命令的 resp 格式
	cli       *Client
	pipelined bool
}
