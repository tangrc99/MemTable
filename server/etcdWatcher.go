package server

import (
	"context"
	"encoding/json"
	"github.com/tangrc99/MemTable/config"
	"github.com/tangrc99/MemTable/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"time"
)

type EtcdWatcher struct {
	ele *concurrency.Election
	cli *clientv3.Client
	clusterWatcher
}

func ETCDWatcherInit() EtcdWatcher {

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"10.0.0.124:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logger.Error("Cluster etcd connection:", err.Error())
	}

	return EtcdWatcher{
		cli: cli,
	}
}

func (e EtcdWatcher) getClusterConfig() clusterConfig {

	ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
	defer cancel()
	res, err := e.cli.Get(ctx, "/cluster_000")
	if err != nil {
		logger.Panic("Cluster etcd no config, info:", err.Error())
	}

	ccfg := clusterConfig{}

	ccfg.ClusterName = config.Conf.ClusterName

	err = json.Unmarshal(res.Kvs[0].Value, &ccfg)
	if err != nil {
		return clusterConfig{}
	}

	return ccfg
}

// func (e *etcdWatcher) watchClusterConfig() <-chan clusterConfig
// func (e *etcdWatcher) watchReplicaStatus() <-chan int {}
// func (e *etcdWatcher) whoIsMaster(string) string      {}

//func (e EtcdWatcher) campaign(shardNum int) {
//
//	// 初始化
//	e.once.Do(func() {
//		session, err := concurrency.NewSession(e.cli)
//		if err != nil {
//			logger.Error("Cluster etcd connection:", err.Error())
//		}
//		prefix := fmt.Sprintf("shard_%05d", shardNum)
//		e.ele = concurrency.NewElection(session, prefix)
//	})
//
//	e.ele.Campaign()
//
//	ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
//	defer cancel()
//	e.ele.Campaign(ctx, "0")
//
//}
