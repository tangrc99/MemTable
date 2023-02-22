package server

type clusterChangeMessage struct {
	timestamp int64 // 每个消息必须带时间戳
	shard     int   // 事件发生的 shard

}

type clusterWatcher interface {

	// 确保返回的是一个有效的配置文件
	getClusterConfig() clusterConfig

	watchClusterConfig() <-chan clusterChangeMessage
	//
	//watchReplicaStatus() <-chan int
	//whoIsMaster(string) string
	//
	campaign() bool
}
