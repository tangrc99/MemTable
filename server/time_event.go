package server

import (
	"MemTable/db/structure"
	"MemTable/logger"
	"time"
)

type EventType int

const (
	// PERIOD 代表周期性事件
	PERIOD EventType = iota
	// SINGLE 代表单次定时事件
	SINGLE
	// IO 代表 IO 事件
	IO
)

type TimeEvent struct {
	executor func()        // 具体事件
	event    EventType     // 类型
	tp       int64         // 时间戳
	period   time.Duration // 周期性事件的周期
}

func NewSingleTimeEvent(exe func(), tp int64) TimeEvent {
	return TimeEvent{
		executor: exe,
		event:    SINGLE,
		tp:       tp,
		period:   -1,
	}
}

func NewPeriodTimeEvent(exe func(), tp int64, period time.Duration) TimeEvent {
	return TimeEvent{
		executor: exe,
		event:    PERIOD,
		tp:       tp,
		period:   period,
	}
}

type TimeEventList struct {
	list *structure.List // 事件链表
}

func NewTimeEventList() *TimeEventList {
	return &TimeEventList{
		list: structure.NewList(),
	}
}

func (events *TimeEventList) AddTimeEvent(event TimeEvent) {

	// 类型检查
	if event.tp < time.Now().Unix() {
		logger.Warning("TimeEventList: add an expired time event")
	}

	if event.event == PERIOD && event.tp <= 0 {
		logger.Error("TimeEventList: period of a period event < 0")
		return
	}

	if events.list.Empty() {
		events.list.PushFront(event)
		return
	}

	for node := events.list.FrontNode(); node != nil; node = node.Next() {
		e, ok := node.Value.(TimeEvent)
		// 列表元素类型不对
		if !ok {

			logger.Error("TimeEventList: type is not TimeEvent")
		}

		if e.tp >= event.tp {
			events.list.InsertBeforeNode(event, node) // fixme : 是否有问题
			return
		}
	}

	// 如果是最大的时间，则需要再尾部加入
	events.list.PushBack(event)
}

// ExecuteOneIfExpire 执行一个任务，如果无可执行任务返回 false
func (events *TimeEventList) ExecuteOneIfExpire() bool {

	// 无任务状态
	if events.list.Empty() {
		logger.Debug("TimeEventList: empty")
		return false
	}

	v := events.list.Front()
	if v == nil {
		logger.Debug("TimeEventList: empty")
		return false
	}

	front, ok := v.(TimeEvent)
	// 列表元素类型不对
	if !ok {
		logger.Error("TimeEventList: type is not TimeEvent")
		events.list.PopFront()
		return false
	}

	if front.tp > time.Now().Unix() {
		return false
	}

	// 弹出任务
	events.list.PopFront()
	// 执行任务
	front.executor()

	// 如果是周期性任务，需要再次定时
	if front.event == PERIOD {
		front.tp = time.Now().Add(front.period).Unix()
		events.AddTimeEvent(front)
	}

	return true
}

func (events *TimeEventList) Size() int {
	return events.list.Size()
}

func (events *TimeEventList) ExecuteManyDuring(duration time.Duration) {
	expired := time.Now().Add(duration).Unix()

	finished := 0

	for expired > time.Now().Unix() && events.ExecuteOneIfExpire() {
		finished++
	}

	if finished > 0 {
		logger.Info("TimeEventList: Finished", finished, "Tasks")
	}
}
