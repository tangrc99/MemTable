package server

import "time"

func TimeEventTest() {
	events := NewTimeEventList()

	events.AddTimeEvent(NewPeriodTimeEvent(func() {
		println("this is a time event")
	}, time.Now().Add(time.Second).Unix(), time.Second))

	time.Sleep(1 * time.Second)

	println(events.ExecuteOneIfExpire())
	println(events.Size())
	println(events.ExecuteOneIfExpire())
	time.Sleep(1 * time.Second)
	println(events.ExecuteOneIfExpire())

	return
}
