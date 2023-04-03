package db

import (
	"github.com/tangrc99/MemTable/db/structure"
	"strings"
)

// 这里借鉴 etcd 的设计，使用两种数据结构来实现发布订阅频道，允许使用目录来进行匹配

// channel 是用于实现 pub/sub 功能的结构体，它维护一个订阅信息表
type channel struct {
	subscriber map[string]*chan []byte
}

// newChannel 创建一个 channel 实例并返回指针
func newChannel() *channel {
	return &channel{
		subscriber: make(map[string]*chan []byte),
	}
}

// subscribe 注册一个订阅信息
func (ch *channel) subscribe(owner string, notify *chan []byte) {
	ch.subscriber[owner] = notify
}

// unSubscribe 删除订阅并且返回删除后的订阅数量
func (ch *channel) unSubscribe(owner string) int {
	delete(ch.subscriber, owner)
	return len(ch.subscriber)
}

// publish 将信息发布到所有的订阅者频道
func (ch *channel) publish(msg []byte) int {
	for _, sub := range ch.subscriber {
		*sub <- msg
	}
	return len(ch.subscriber)
}

func (ch *channel) Cost() int64 {

	//TODO:
	return -1
}

// Channels 维护所有的订阅频道信息，内部有哈希表和前缀树两种数据结构，分别用于
// 处理单一频道和路径频道两种模式的发布和订阅。
type Channels struct {
	channels map[string]*channel
	paths    *structure.TrieTree
}

// NewChannels 创建一个 Channel 实例并返回指针
func NewChannels() *Channels {
	return &Channels{
		channels: make(map[string]*channel),
		paths:    structure.NewTrieTree(),
	}
}

// Publish 发布消息到指定的频道上，如果频道是一个路径，消息将会被发送到该路径以及路径下的一级子目录频道
func (chs *Channels) Publish(channel string, msg []byte) int {

	if channel[0] == '/' {
		return chs.publishPath(channel, msg)
	}

	ch, ok := chs.channels[channel]
	if !ok {
		return 0
	}

	return ch.publish(msg)
}

func (chs *Channels) publishPath(ch string, msg []byte) int {

	paths := strings.Split(ch, "/")

	nodes := chs.paths.AllLeafNodeInPath(paths[1:])
	pubs := 0
	for _, node := range nodes {
		c := node.Value.(*channel)
		pubs += c.publish(msg)
	}
	return pubs
}

// Subscribe 订阅指定的频道，如果频道是一个路径，那么同时也会接收上一级父目录发布的消息
func (chs *Channels) Subscribe(channel string, owner string, notify *chan []byte) {

	if channel[0] == '/' {
		chs.subscribePath(channel, owner, notify)
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

func (chs *Channels) subscribePath(ch string, owner string, notify *chan []byte) {

	paths := strings.Split(ch, "/")

	// 如果不存在，则创建一个新的 channel
	node, _ := chs.paths.AddNodeIfNotLeaf(paths[1:], newChannel())

	node.Value.(*channel).subscribe(owner, notify)
}

// UnSubscribe 取消指定频道的订阅
func (chs *Channels) UnSubscribe(channel string, owner string) bool {
	if channel[0] == '/' {
		return chs.unSubscribePath(channel, owner)
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

func (chs *Channels) unSubscribePath(ch string, owner string) bool {
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
