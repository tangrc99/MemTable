package db

import "MemTable/db/structure"

// 这里借鉴 etcd 的设计，使用两种数据结构来实现发布订阅频道，允许使用目录来进行匹配

type channel struct {
	subscriber map[string]*chan []byte
}

func newChannel() *channel {
	return &channel{
		subscriber: make(map[string]*chan []byte),
	}
}

func (ch *channel) subscribe(owner string, notify *chan []byte) {
	ch.subscriber[owner] = notify
}

// unSubscribe 删除订阅并且返回删除后的订阅数量
func (ch *channel) unSubscribe(owner string) int {
	delete(ch.subscriber, owner)
	return len(ch.subscriber)
}

type Channels struct {
	channels map[string]*channel
	paths    *structure.TrieTree
}

func NewChannels() *Channels {
	return &Channels{
		channels: make(map[string]*channel),
		paths:    structure.NewTrieTree(),
	}
}

func (chs *Channels) Publish(channel string, msg []byte) int {
	ch, ok := chs.channels[channel]
	if !ok {
		return 0
	}
	for _, sub := range ch.subscriber {
		*sub <- msg
	}
	return len(ch.subscriber)
}

func (chs *Channels) Subscribe(channel string, owner string, notify *chan []byte) {
	ch, ok := chs.channels[channel]
	// 不存在则创建
	if !ok {
		ch = newChannel()
		chs.channels[channel] = ch
	}
	ch.subscribe(owner, notify)
}

func (chs *Channels) UnSubscribe(channel string, owner string) bool {
	ch, ok := chs.channels[channel]
	if !ok {
		return false
	}

	if ch.unSubscribe(owner) == 0 {
		delete(chs.channels, channel)
	}

	return true
}
