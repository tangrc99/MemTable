package server

import (
	"encoding/json"
)

const (
	MNewLeader = iota
	MAnnounce
)

type clusterChangeMessage struct {
	Timestamp int64  `json:"timestamp,omitempty" ` // 每个消息必须带时间戳，不同情况下使用的时间戳不一样
	Shard     int    `json:"shard"`                // 事件发生的 shard
	EType     int    `json:"type"`                 // 事件类型
	Content   string `json:"content"`              // 事件内容
}

func generateNewLeaderMessage(shard int, newLeader string) string {
	msg := clusterChangeMessage{
		Shard:   shard,
		EType:   MNewLeader,
		Content: newLeader,
	}
	marshal, err := json.Marshal(msg)
	if err != nil {
		return ""
	}
	return string(marshal)
}

func generateAnnounceMessage(shard int, content string) string {
	msg := clusterChangeMessage{
		Shard:   shard,
		EType:   MAnnounce,
		Content: content,
	}

	marshal, err := json.Marshal(msg)
	if err != nil {
		return ""
	}
	return string(marshal)
}
