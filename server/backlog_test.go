package server

import (
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/logger"
	"testing"
)

func TestBackLog(t *testing.T) {

	_ = logger.Init("", "", logger.WARNING)

	s := NewServer()
	s.standAloneToMaster()

	assert.Equal(t, uint64(0), s.backLog.LowWaterLevel())

	event := &Event{raw: []byte("sdfsdfsdfds"), cli: NewClient(nil)}
	assert.Equal(t, uint64(0), s.backLog.LowWaterLevel())

	s.appendBackLog(event)
	assert.Equal(t, uint64(0), s.backLog.LowWaterLevel())

	assert.Equal(t, uint64(34), s.backLog.HighWaterLevel())

	rd := s.backLog.Read(0, 34)

	assert.Equal(t, []byte("*2\r\n$6\r\nselect\r\n$1\r\n0\r\nsdfsdfsdfds"), rd)
}
