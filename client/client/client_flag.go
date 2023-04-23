package client

import (
	"github.com/tangrc99/MemTable/resp"
	"net"
)

const (
	disconnected = 0x0
	connected    = 0x1
	blocked      = 0x10
	inTx         = 0x100
)

func (c *Client) toDisconnected() {
	c.flag = disconnected
}

func (c *Client) toConnected(conn net.Conn) {
	c.flag = connected
	c.conn = conn
	c.parser = resp.NewParser(c.conn)
}

func (c *Client) toBlocked() {
	c.flag |= blocked | connected
}

func (c *Client) toInTx() {
	c.flag |= inTx | connected
}

func (c *Client) toNotInTx() {
	c.flag &^= inTx
}

func (c *Client) isInTx() bool {
	return c.flag&inTx != 0
}

func (c *Client) isConnected() bool {
	return c.flag&connected != 0
}
func (c *Client) isBlocked() bool {
	return c.flag&blocked != 0
}
