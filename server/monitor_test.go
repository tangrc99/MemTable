package server

import (
	"testing"
)

func TestMonitor(t *testing.T) {
	m := NewMonitor()
	e := &Event{
		cmd: [][]byte{[]byte("14"), []byte("fds")},
		cli: NewClient(nil),
	}

	m.NotifyAll(e)

	m.Stop()

}
