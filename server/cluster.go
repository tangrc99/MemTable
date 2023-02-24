package server

import (
	"fmt"
	"github.com/tangrc99/MemTable/config"
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

func newSelfNode(name string) *clusterNode {
	node :=
		&clusterNode{name: name}
	node.slaveOf = node
	return node
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
	return n.slaveOf == n
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
	n.slaveOf = n
}

func (n *clusterNode) toString() string {

	ret := "{ name: " + n.name + " ,\n" + "slaves: ["
	for _, slave := range n.slaves {
		ret += slave.name + ","
	}
	ret += "]\n"
	if n.slaveOf != nil {
		ret += "slaveof: " + n.slaveOf.name + "}"
	}
	return ret
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
	c.self = newSelfNode(self.url)
	c.nodes = make(map[string]*clusterNode)
	c.nodes[c.server.url] = c.self
	c.state = ClusterInit
	c.watcher = ETCDWatcherInit(config.Conf.ClusterName, c.server.url)
	c.config = c.watcher.getClusterConfig()
	c.slots = make([]*clusterNode, slotNum)

	c.countClusterNodeNum()
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
		for _, name := range shard {

			if name == c.self.name {

				c.self.slaveOfNode(c.nodes[shard[0]])

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
		// 自身主从复制是正常的，处理所有到达的事务。由于集群事务很少，因此不会阻塞很久。
		// 如果集群异常，也会被限制处理消息个数

		for finished, singleHandle := 0, 5; finished < singleHandle; {
			select {
			case m := <-c.msg:
				// 处理单个事件
				c.handleClusterChangeMessage(&m)
				finished++
			default:
				finished = singleHandle
			}
		}

	case ClusterDown:
		// 如果主从复制中发现主节点下线，那么集群状态会变更为 ClusterDown

		// 先处理集群事务，如果其中有集群新主的通知，那么从故障中恢复
		for finished := false; !finished; {
			select {
			case m := <-c.msg:
				// 处理单个事件
				c.handleClusterChangeMessage(&m)
			default:
				finished = true
			}
		}

		// 这里可能已经成功完成故障恢复
		if c.state == ClusterOK {
			return
		}

		// 如果没有通知，则尝试进行选举。
		// 若选举成功，则切换自身为主节点，选举失败不需要做任何事情。
		ok := c.watcher.campaign()
		if ok {

			updateShardMaster(c.self.slaveOf, c.self)

			// 升级为主节点，等待其他节点的连接
			c.server.slaveToStandAlone()
			c.state = ClusterOK

		} else {
			// 竞选失败，不需要做任何事情
			// 阻塞一段时间，防止频繁发起选举。

		}

	}
	println(c.allNodes())
}

// initClusterConn 连接配置文件中尚未连接的节点，当完成所有节点的连接后，会将集群状态更改为 ClusterOK
func (c *clusterStatus) initClusterConn() {

	logger.Info("Cluster Try to connect to other nodes")

	for i := 0; i < c.config.ShardNum; i++ {

		if _, exist := c.nodes[c.config.Shards[i][0]]; !exist {

			// 连接并且分配 slot
			// FIXME: 不仅仅需要连接，还需要握手
			cnn, err := net.DialTimeout("tcp", c.config.Shards[i][0], 1*time.Second)

			if err != nil {
				logger.Error("Cluster init connect to peer failed:", c.config.Shards[i][0])
				continue
			}
			peer := acceptNewClusterNode(cnn)
			c.nodes[c.config.Shards[i][0]] = peer
			peer.slaveOf = peer
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

		shardMaster, exist := c.nodes[c.config.Shards[i][0]]

		// 先连接到主节点，再连接到 slave 节点
		if !exist {
			continue
		}

		for j := 1; j < len(c.config.Shards[i]); j++ {

			if _, exist := c.nodes[c.config.Shards[i][j]]; exist {
				continue
			}

			cnn, err := net.DialTimeout("tcp", c.config.Shards[i][j], 1*time.Second)
			if err != nil {
				logger.Error("Cluster init connect to peer failed:", c.config.Shards[i][j])
				continue
			}

			logger.Info("Cluster connected to peer:", c.config.Shards[i][j])

			peer := acceptNewClusterNode(cnn)
			c.nodes[c.config.Shards[i][j]] = peer
			peer.slaveOfNode(shardMaster)
		}
	}

	if c.configNodeNum == len(c.nodes) {
		logger.Info("Cluster : Connected to all config nodes")
		// 给自身节点分配 slot，这里会覆盖掉之前分配给主节点的slot保证 slave 可以处理读
		c.initLocalShard()

		// 开始监视集群中是否发生变动
		c.msg = c.watcher.watchClusterChanges()
		c.state = ClusterOK
		ok := c.watcher.initCampaign(c.selfShard, c.self.isMaster())

		if !c.self.isMaster() || !ok {

			// 休眠一段时间，让主节点完成竞选
			time.Sleep(time.Second)
			// FIXME : 还需要考虑其他的分片上的主节点变更问题

			// 当前主节点可能与配置主节点不同，再次问询当前集中的状态来确保配置正确
			for i := 0; i < 3; i++ {
				master := c.watcher.whoIsMaster()
				if master == "" {
					time.Sleep(time.Second)
					continue
				}
				node := c.nodes[master]
				if node != c.self.slaveOf {
					updateShardMaster(c.self.slaveOf, node)
				}
				c.server.sendSyncToMaster(master)
				return
			}

			logger.Warning("Cluster No Master Now,shard:", c.selfShard)
			// 如果多次询问仍没有主节点，尝试竞选
			c.state = ClusterDown
		}
	}

}

// handleClusterChangeMessage 处理已经到达的集群事件，事件可能为发生了主变更
func (c *clusterStatus) handleClusterChangeMessage(msg *clusterChangeMessage) {

	if msg == nil {
		logger.Error("Cluster Nil Pointer of clusterChangeMessage")
		return
	}

	switch msg.EType {
	case MNewLeader, MAnnounce:

		// 更改配置中的 leader
		leader, exist := c.nodes[msg.Content]
		if !exist {
			logger.Error(fmt.Sprintf("Cluster nonexistent node become leader, shard %d node %s", msg.Shard, msg.Content))
		}

		// 更新自身视图
		updateShardMaster(leader.slaveOf, leader)

		if msg.Shard != c.selfShard {

			c.assignShardToNode(msg.Shard, leader)

		} else {

			// 其他节点竞选成功，自身更换为上线状态
			if msg.Content != c.self.name {
				c.server.SendPSyncToMaster(msg.Content)
				c.state = ClusterOK
			}
		}

	}
}

func (c *clusterStatus) assignShardToNode(shardNum int, node *clusterNode) {
	// 更改 shard 上的所有 slot
	shardWidth := slotNum / c.config.ShardNum
	start := shardNum * shardWidth
	end := start + shardWidth
	if shardNum == c.config.ShardNum-1 {
		end = slotNum
	}
	for j := start; j < end; j++ {
		c.slots[j] = node
	}
}

func updateShardMaster(old, new *clusterNode) {
	if old == new {
		return
	}

	if old == nil {
		logger.Error("Cluster NIL Old Leader")
	}
	if new == nil {
		logger.Error("Cluster NIL New Leader")
	}

	for _, slave := range old.slaves {
		if slave != new {
			slave.slaveOf = new
			new.slaves = append(new.slaves, slave)
		}
	}
	// 清空主节点的 slaves
	old.slaves = nil
	old.slaveOf = new
	// 清空自身的 slave of
	new.slaveOf = new

}

func (c *clusterStatus) allNodes() string {
	ret := ""
	for _, v := range c.nodes {
		ret += v.toString() + "\n"
	}
	return ret
}
