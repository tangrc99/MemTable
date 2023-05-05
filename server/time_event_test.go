package server

import (
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/logger"
	"github.com/tangrc99/MemTable/server/global"
	"testing"
	"time"
)

func TestEventList(t *testing.T) {

	_ = logger.Init("", "", logger.WARNING)
	logger.Disable()

	global.UpdateGlobalClock()

	tl := NewTimeEventList()

	assert.False(t, tl.ExecuteOneIfExpire(global.Now))

	tl.AddTimeEvent(NewPeriodTimeEvent(func() {}, global.Now.Add(time.Second).Unix(), time.Second))
	tl.AddTimeEvent(NewPeriodTimeEvent(func() {}, global.Now.Add(time.Second).Unix(), -1*time.Second))
	tl.AddTimeEvent(NewSingleTimeEvent(func() {}, global.Now.Add(time.Second).Unix()))

	assert.Equal(t, 2, tl.Size())

	global.Now = global.Now.Add(time.Second)

	assert.True(t, tl.ExecuteOneIfExpire(global.Now))
	assert.True(t, tl.ExecuteOneIfExpire(global.Now))

	assert.Equal(t, 1, tl.Size())

	global.Now = global.Now.Add(time.Second)

	assert.Equal(t, 0, tl.ExecuteManyDuring(global.Now, 0))

	assert.Equal(t, 1, tl.ExecuteManyDuring(global.Now, time.Second))
}
