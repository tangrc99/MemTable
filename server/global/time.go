package global

import "time"

// Now 是全局时钟，由于使用精准时钟是一个非常耗时的操作，所以使用一个全局时钟。
// 每一次 EventLoop 会更新一次全局时钟。
var Now time.Time

// UpdateGlobalClock 更新全局时钟 global.Now
func UpdateGlobalClock() {
	Now = time.Now()
}

// RealTime 会更新 global.Now 并返回
func RealTime() time.Time {
	Now = time.Now()
	return Now
}
