package server

import (
	"encoding/json"
)

type clusterNodeJson struct {
	Name    string   `json:"name"`
	Slaves  []string `json:"slaves"`
	SlaveOf string   `json:"slave_of"`
}

func newClusterNodeJson(n *clusterNode) *clusterNodeJson {
	nj := clusterNodeJson{
		Name:   n.name,
		Slaves: make([]string, 0, len(n.slaves)),
	}

	for _, slave := range n.slaves {
		nj.Slaves = append(nj.Slaves, slave.name)
	}

	if n.slaveOf != nil {
		nj.SlaveOf = n.slaveOf.name
	}
	return &nj
}

type clusterJson struct {
	Config     *clusterConfig     `json:"config"`
	AliveNodes int                `json:"alive_nodes"`
	State      clusterState       `json:"state"`
	Masters    []*clusterNodeJson `json:"masters"`
}

func newClusterJson(s *clusterStatus) *clusterJson {
	j := &clusterJson{
		Config:     &s.config,
		AliveNodes: s.aliveNodesNum(),
		State:      s.state,
		Masters:    make([]*clusterNodeJson, 0, s.config.ShardNum),
	}
	for _, n := range s.nodes {
		if n.isMaster() {
			j.Masters = append(j.Masters, newClusterNodeJson(n))
		}
	}
	return j
}

const (
	MNewLeader = iota
	MAnnounce
	MNodeUp
	MNodeDown
)

type clusterChangeMessage struct {
	Timestamp int64  `json:"timestamp,omitempty"` // 每个消息必须带时间戳，不同情况下使用的时间戳不一样
	Shard     int    `json:"shard"`               // 事件发生的 shard
	EType     int    `json:"type"`                // 事件类型
	Content   string `json:"content"`             // 事件内容
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

func generateNodeDownMessage(shard int, content string) string {

	msg := clusterChangeMessage{
		Shard:   shard,
		EType:   MNodeDown,
		Content: content,
	}

	marshal, err := json.Marshal(msg)
	if err != nil {
		return ""
	}
	return string(marshal)
}

func generateNodeUpMessage(shard int, content string) string {

	msg := clusterChangeMessage{
		Shard:   shard,
		EType:   MNodeUp,
		Content: content,
	}

	marshal, err := json.Marshal(msg)
	if err != nil {
		return ""
	}
	return string(marshal)
}
