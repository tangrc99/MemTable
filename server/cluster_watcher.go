package server

type clusterWatcher interface {

	// getClusterConfig 尝试获取集群的配置文件，确保返回的是一个有效的配置文件
	getClusterConfig() clusterConfig

	// watchClusterConfig 将会开启一个后台协程来监控集群的状态，
	// 若集群状态发生变更，会将变更消息通过 channel 传递
	watchClusterChanges() <-chan clusterChangeMessage

	// whoIsMaster 将会阻塞地回去当前 shard 内的主节点，如果当前 shard 不存在主节点，返回""
	whoIsMaster() string

	// initCampaign 用来初始化选举的相关信息
	initCampaign(shardNum int, isMaster bool) bool

	// campaign 用于当 shard 内主节点下线时进行选举
	campaign() bool

	// announce 周期性地向集群宣布自身是主节点，这是为了防止刚刚上线的节点没有更新自身的视图
	announce()

	// doSomething 将会被周期性地调用，如果集群需要额外做一些工作，可以在这里实现
	doSomething(any)
}
