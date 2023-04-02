package server

// clusterWatcher 负责处理集群信息的发布与订阅
type clusterWatcher interface {

	// getClusterConfig 尝试获取集群的配置文件，确保返回的是一个有效的配置文件
	getClusterConfig() clusterConfig

	// watchClusterChanges 将会开启一个后台协程来监控集群的状态，
	// 若集群状态发生变更，会将变更消息通过 channel 传递
	watchClusterChanges() <-chan clusterChangeMessage

	// whoIsMaster 将会阻塞地回去当前 shard 内的主节点，如果当前 shard 不存在主节点，返回""
	whoIsMaster() string

	// initCampaign 用来初始化选举的相关信息
	initCampaign(shardNum int, isMaster bool) bool

	// campaign 用于当 shard 内主节点下线时进行选举
	campaign() bool

	// leaderAnnounce 周期性地向集群宣布自身是主节点，宣布已经下线的节点。这是为了防止刚刚上线的节点没有更新自身的视图；
	// 只有主节点会宣布已下线节点，当主节点下线后，只有集群内部完成选举，新主节点才会向集群宣布旧主下线
	leaderAnnounce(nodes []string)

	// upNodeAnnounce 会宣布上线的节点。每个上线的节点只会被主节点宣布一次，即使有部分节点未收到该消息，
	// 也不会影响集群的工作状态
	upNodeAnnounce(node string)

	// doSomething 将会被周期性地调用，如果集群需要额外做一些工作，可以在这里实现
	doSomething()
}
