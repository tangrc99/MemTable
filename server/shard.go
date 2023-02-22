package server

import (
	"github.com/tangrc99/MemTable/logger"
	"github.com/tangrc99/MemTable/utils"
	"net"
	"time"
)

const slotNum = 10 //1 << 14

type clusterNode struct {
	name string

	peer     *Client   // 代表对端的 client
	pingTime time.Time // 上次发送信息的时间
	pongTime time.Time // 上次收到信息的时间
	slaves   []*clusterNode
	slaveOf  *clusterNode
}

func acceptNewClusterNode(conn net.Conn) *clusterNode {
	node := &clusterNode{
		name:     conn.RemoteAddr().String(),
		peer:     NewClient(conn),
		pingTime: time.Now(),
		pongTime: time.Now(),
		slaves:   make([]*clusterNode, 0),
	}
	return node
}

func (n *clusterNode) isMaster() bool {
	return n.slaveOf == nil
}

// slaveOfNode 修改当前节点中的状态，不会发送命令给对应的节点
func (n *clusterNode) slaveOfNode(master *clusterNode) {
	if master == nil {
		logger.Error("Cluster: Add Slave To Nil Node")
		return
	} else if !master.isMaster() {
		logger.Error("Cluster: Add Slave To A Slave Node", master.name)
		return
	}
	n.slaveOf = master
	master.slaves = append(master.slaves, n)
}

// slaveOfNone 修改当前节点中的状态，不会发送命令给对应的节点
func (n *clusterNode) slaveOfNone() {
	n.slaveOf = nil
}

/* ---------------------------------------------------------------------------
* 配置以及状态
* ------------------------------------------------------------------------- */

type clusterConfig struct {
	ClusterName string     `json:"cluster_name,omitempty"`
	ShardNum    int        `json:"shard_num,omitempty"`
	Shards      [][]string `json:"shards,omitempty"`
}

type clusterState int

const (
	ClusterNone clusterState = iota
	ClusterInit
	ClusterOK
	ClusterDown
)

/* ---------------------------------------------------------------------------
* 集群状态
* ------------------------------------------------------------------------- */

type clusterStatus struct {
	server *Server

	self      *clusterNode
	selfShard int

	state clusterState
	slots []*clusterNode //

	nodes map[string]*clusterNode

	configNodeNum int // 配置中的节点数量

	// 高可用相关
	config  clusterConfig
	watcher clusterWatcher

	msg <-chan clusterChangeMessage
}

// initCluster 初始化集群的各个状态
func (c *clusterStatus) initCluster(self *Server) {
	c.server = self
	c.self = &clusterNode{name: self.url}
	c.nodes = make(map[string]*clusterNode)
	c.nodes[c.server.url] = c.self
	c.state = ClusterInit
	c.watcher = ETCDWatcherInit()
	c.config = c.watcher.getClusterConfig()
	c.slots = make([]*clusterNode, slotNum)

}

// getSlot 根据键值来计算出所在的哈希槽
func (c *clusterStatus) getSlot(key string) int {
	return utils.HashKey(key) % slotNum
}

// countClusterNodeNum 计算配置中一共有多少个节点
func (c *clusterStatus) countClusterNodeNum() {
	for _, shard := range c.config.Shards {
		c.configNodeNum += len(shard)
	}
}

// initLocalShard 根据配置文件判断自身在第几个 shard 中，一个节点一定会负责一定的 slot 哪怕他是从节点，这是为了防止请求全部转发到主节点
func (c *clusterStatus) initLocalShard() {

	for i, shard := range c.config.Shards {
		for j, name := range shard {

			if name == c.self.name {
				// 如果在配置中不是主节点，需要更改自身的状态
				if j != 0 {
					c.self.slaveOf = c.nodes[shard[0]]
				}
				c.selfShard = i
				goto finished
			}
		}
	}

finished:
	shardWidth := slotNum / c.config.ShardNum
	start := c.selfShard * shardWidth
	end := start + shardWidth
	for j := start; j < end; j++ {
		c.slots[j] = c.self
	}

	if !c.self.isMaster() {
		c.server.sendSyncToMaster(c.self.slaveOf.name)
	}
}

// isKeyNeedMove 判断键值是否在当前节点的 slot 中
func (c *clusterStatus) isKeyNeedMove(key string) (bool, int, *clusterNode) {
	if c.state == ClusterNone {
		return false, -1, nil
	}

	slot := utils.HashKey(key) % slotNum
	node := c.slots[utils.HashKey(key)%slotNum]

	println(slot)

	if node != c.self {
		return true, slot, node
	}
	return false, -1, nil

}

// clusterNodeNum 当前集群节点数量
func (c *clusterStatus) clusterNodeNum() int {
	return len(c.nodes)
}

// handleClusterEvents 负责完成 cluster 集群中的一些周期性任务，该函数被放入了时间链表中
func (c *clusterStatus) handleClusterEvents() {

	switch c.state {
	case ClusterNone:
		return
	case ClusterInit:
		c.initClusterConn()
	case ClusterOK:

		// 这里只需要处理其他节点中的变更，如果自身有变更，状态会切换为 ClusterDown 状态
		//select {
		//case m := <-c.msg:
		//
		//default:
		//
		//}

	case ClusterDown:
		// 这里已经发现主节点下线，需要在 shard 内部处理消息
		ok := c.watcher.campaign()
		if ok {
			for _, slave := range c.self.slaveOf.slaves {
				if slave != c.self {
					slave.slaveOf = c.self
					c.self.slaves = append(c.self.slaves, slave)
				}
			}
			// 清空主节点的 slaves
			c.self.slaveOf.slaves = nil
			// 清空自身的 slave of
			c.self.slaveOf = nil

			// 升级为主节点，等待其他节点的连接
			c.server.slaveToStandAlone()

		} else {
			// 竞选失败，连接到主节点
		}
	}
}

// initClusterConn 连接配置文件中尚未连接的节点，当完成所有节点的连接后，会将集群状态更改为 ClusterOK
func (c *clusterStatus) initClusterConn() {

	for i := 0; i < c.config.ShardNum; i++ {
		if _, exist := c.nodes[c.config.Shards[i][0]]; !exist {
			// 连接并且分配 slot

			cnn, err := net.DialTimeout("tcp", c.config.Shards[i][0], 1*time.Second)
			if err != nil {
				logger.Error("Cluster init connect to peer failed:", c.config.Shards[i][0])
				continue
			}
			peer := acceptNewClusterNode(cnn)
			c.nodes[c.config.Shards[i][0]] = peer

			// 计算出需要分配的 slot 范围
			shardWidth := slotNum / c.config.ShardNum
			start := i * shardWidth
			end := start + shardWidth
			if i == c.config.ShardNum-1 {
				end = slotNum
			}
			for j := start; j < end; j++ {
				c.slots[j] = peer
			}

		}
		for j := 1; j < len(c.config.Shards[i]); j++ {

			cnn, err := net.DialTimeout("tcp", c.config.Shards[i][0], 1*time.Second)
			if err != nil {
				logger.Error("Cluster init connect to peer failed:", c.config.Shards[i][0])
				continue
			}
			peer := acceptNewClusterNode(cnn)
			c.nodes[c.config.Shards[i][j]] = peer
			c.nodes[c.config.Shards[i][0]].slaves = append(c.nodes[c.config.Shards[i][0]].slaves, peer)
		}

	}

	if c.configNodeNum == len(c.nodes) {

		// 给自身节点分配 slot，这里会覆盖掉之前分配给主节点的slot保证 slave 可以处理读
		c.initLocalShard()

		// 开始监视集群中是否发生变动
		c.msg = c.watcher.watchClusterConfig()
		c.state = ClusterOK
	}
}
