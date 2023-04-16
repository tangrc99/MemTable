package db

import (
	"github.com/gofrs/uuid"
	"github.com/tangrc99/MemTable/db/structure"
	"github.com/tangrc99/MemTable/server/global"
	"unsafe"
)

// consumer 是一个消费者，它包含了一个序列号与一个用于接受消息的 channel；可以认为这是数据库视角下的客户端
type consumer struct {
	id       uuid.UUID
	notifier chan<- []byte
	deadline int64 // 有效期
}

// newConsumer 创建一个 consumer 对象，id 是对应客户端的 id，n 是客户端的通知 channel，ddl 是 consumer 存活时间，-1 代表无存活时间
func newConsumer(id uuid.UUID, n chan<- []byte, ddl int64) *consumer {
	return &consumer{
		id:       id,
		notifier: n,
		deadline: ddl,
	}
}

func (c *consumer) Cost() int64 {
	return 48 // 24 + 16 + 8
}

const blockMapBasicCost = int64(unsafe.Sizeof(blockMap{}))

// blockMap 存储因 blpop 或 brpop 命令而阻塞的客户端信息
type blockMap struct {
	consumers map[string]*structure.List
	keyCost   int64
}

func newBlockMap() *blockMap {
	return &blockMap{
		consumers: make(map[string]*structure.List),
		keyCost:   0,
	}
}

func (c *blockMap) register(key string, id uuid.UUID, n chan<- []byte, ddl int64) {
	l, exist := c.consumers[key]
	if !exist {
		l = structure.NewList()
		c.consumers[key] = l
		c.keyCost += int64(len(key))
	}

	// 检查是否有重复注册
	for node := l.FrontNode(); node != nil; node = node.Next() {
		if node.Value.(*consumer).id == id {
			node.Value = newConsumer(id, n, ddl)
			return
		}
	}

	l.PushBack(newConsumer(id, n, ddl))
}

func (c *blockMap) unregister(key string, id uuid.UUID) {
	l, exist := c.consumers[key]
	if !exist {
		return
	}
	for n := l.FrontNode(); n != nil; n = n.Next() {
		if n.Value.(*consumer).id == id {
			l.RemoveNode(n)
			return
		}
	}
}

func (c *blockMap) tryConsume(key string, message []byte) bool {
	l, exist := c.consumers[key]
	if !exist || l.Empty() {
		return false
	}

	for {
		v := l.PopFront()
		if v == nil {
			delete(c.consumers, key)
			return false
		}
		n := v.(*consumer)
		if n.deadline < 0 || n.deadline > global.Now.Unix() {
			n.notifier <- message
			break
		}
	}

	if l.Empty() {
		delete(c.consumers, key)
		c.keyCost -= int64(len(key))
	}

	return true
}

// Cost is O(n)
func (c *blockMap) Cost() int64 {
	cost := int64(0)
	for _, v := range c.consumers {
		cost += v.Cost()
	}
	return blockMapBasicCost + cost
}
