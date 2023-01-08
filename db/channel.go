package db

import (
	"github.com/tangrc99/MemTable/db/structure"
	"strings"
)

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

func (ch *channel) Publish(msg []byte) int {
	for _, sub := range ch.subscriber {
		*sub <- msg
	}
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

	if channel[0] == '/' {
		return chs.PublishPath(channel, msg)
	}

	ch, ok := chs.channels[channel]
	if !ok {
		return 0
	}

	return ch.Publish(msg)
}

func (chs *Channels) PublishPath(ch string, msg []byte) int {

	paths := strings.Split(ch, "/")

	nodes := chs.paths.AllLeafNodeInPath(paths[1:])
	pubs := 0
	for _, node := range nodes {
		c := node.Value.(*channel)
		pubs += c.Publish(msg)
	}
	return pubs
}

func (chs *Channels) Subscribe(channel string, owner string, notify *chan []byte) {

	if channel[0] == '/' {
		chs.SubscribePath(channel, owner, notify)
		return
	}

	ch, ok := chs.channels[channel]
	// 不存在则创建
	if !ok {
		ch = newChannel()
		chs.channels[channel] = ch
	}
	ch.subscribe(owner, notify)
}

func (chs *Channels) SubscribePath(ch string, owner string, notify *chan []byte) {

	paths := strings.Split(ch, "/")

	// 如果不存在，则创建一个新的 channel
	node, _ := chs.paths.AddNodeIfNotLeaf(paths[1:], newChannel())

	node.Value.(*channel).subscribe(owner, notify)
}

func (chs *Channels) UnSubscribe(channel string, owner string) bool {
	if channel[0] == '/' {
		return chs.UnSubscribePath(channel, owner)
	}
	ch, ok := chs.channels[channel]
	if !ok {
		return false
	}

	if ch.unSubscribe(owner) == 0 {
		delete(chs.channels, channel)
	}

	return true
}

func (chs *Channels) UnSubscribePath(ch string, owner string) bool {
	paths := strings.Split(ch, "/")

	node, ok := chs.paths.GetLeafNode(paths[1:])
	if !ok {
		return false
	}
	subs := node.Value.(*channel).unSubscribe(owner)
	if subs == 0 {
		chs.paths.DeleteLeafNode(node)
	}
	return true
}
