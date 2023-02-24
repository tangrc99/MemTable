package server

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/tangrc99/MemTable/config"
	"github.com/tangrc99/MemTable/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"time"
)

type EtcdWatcher struct {
	clusterName string
	shard       int
	shardName   string
	host        string

	ele         *concurrency.Election
	electionMsg <-chan clientv3.GetResponse
	cli         *clientv3.Client
	clusterWatcher
}

func ETCDWatcherInit(clusterName string, host string) *EtcdWatcher {

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"10.0.0.124:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logger.Error("Cluster etcd connection:", err.Error())
	}

	return &EtcdWatcher{
		clusterName: clusterName,
		cli:         cli,
		host:        host,
	}
}

func (e *EtcdWatcher) getClusterConfig() clusterConfig {

	ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
	defer cancel()
	res, err := e.cli.Get(ctx, fmt.Sprintf("/%s", e.clusterName))

	if err != nil {
		logger.Panic("Cluster etcd pull config error, info:", err.Error())
	}
	if len(res.Kvs) < 1 {
		logger.Panic("Cluster etcd empty config path:", fmt.Sprintf("/%s", e.clusterName))
	}

	ccfg := clusterConfig{}

	ccfg.ClusterName = config.Conf.ClusterName

	err = json.Unmarshal(res.Kvs[0].Value, &ccfg)
	if err != nil {
		return clusterConfig{}
	}

	return ccfg
}

// initCampaign 如果失败，可能是由于该节点掉线后重连；这时候需要自动降级为副节点。
func (e *EtcdWatcher) initCampaign(shardNum int, isMaster bool) bool {

	e.shardName = fmt.Sprintf("shard_%d", shardNum)
	e.shard = shardNum

	// 更改租约时间，防止因为 Lua 慢脚本的运行而下线
	session, err := concurrency.NewSession(e.cli, concurrency.WithTTL(6))

	if err != nil {
		logger.Error("Cluster etcd connection:", err.Error())
	}
	prefix := fmt.Sprintf("/%s/election/%s", e.clusterName, e.shardName)

	e.ele = concurrency.NewElection(session, prefix)

	logger.Info("Cluster connect to etcd election channel")

	if isMaster {
		ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
		defer cancel()
		electErr := e.ele.Campaign(ctx, e.host)

		if electErr != nil {
			logger.Error("Cluster etcd: Master's initial election failed")
			return false
		}
		logger.Info("Cluster Campaign Succeed, shard: ", e.shardName)

	}

	return true
}

func (e *EtcdWatcher) watchClusterChanges() <-chan clusterChangeMessage {

	ntf := make(chan clusterChangeMessage, 20)

	wchan := e.cli.Watch(context.TODO(), e.publishChannel())

	go func() {

		for true {

			select {
			case msg := <-wchan:

				if msg.Err() != nil {
					logger.Panic("Cluster publish channel error", msg.Err().Error())
				}

				for _, event := range msg.Events {

					content := event.Kv.Value
					m := clusterChangeMessage{}
					err := json.Unmarshal(content, &m)

					if err != nil {
						logger.Error(fmt.Sprintf("Cluster Wrong Change Message, %s", string(content)))
						continue
					}

					logger.Info("Cluster New Change Message:", string(content))

					m.Timestamp = event.Kv.Version

					ntf <- m
				}

			default:
				time.Sleep(200 * time.Millisecond)
			}
		}

	}()

	return ntf
}

// func (e *etcdWatcher) watchReplicaStatus() <-chan int {}
func (e *EtcdWatcher) whoIsMaster() string {

	ret, err := e.ele.Leader(context.TODO())
	if err != nil {
		logger.Error("Cluster No Master")
		return ""
	}
	leader := string(ret.Kvs[0].Value)
	return leader
}

func (e *EtcdWatcher) campaign() bool {

	ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
	defer cancel()
	err := e.ele.Campaign(ctx, e.host)

	if err != nil {
		logger.Info("Cluster Campaign Failed, reason", err.Error())
		return false
	}
	logger.Info("Cluster Campaign Succeed, shard: ", e.shardName)

	// 如果成功了，需要在广播 channel 告知全部节点
	pCh := e.publishChannel()

	msg := generateNewLeaderMessage(e.shard, e.host)

retry:
	_, err = e.cli.Put(context.TODO(), pCh, msg)
	if err != nil {
		logger.Error("Cluster Publish Message Error, Info", err.Error())
		goto retry
	}

	return true
}

func (e *EtcdWatcher) announce() {

	// 如果成功了，需要在广播 channel 告知全部节点
	pCh := e.publishChannel()

	msg := generateAnnounceMessage(e.shard, e.host)

	_, err := e.cli.Put(context.TODO(), pCh, msg)
	if err != nil {
		logger.Error("Cluster Publish Message Error, Info", err.Error())
	}

}

func (e *EtcdWatcher) publishChannel() string {
	return fmt.Sprintf("/%s/channel", e.clusterName)
}

func (e *EtcdWatcher) electionChannel() string {
	return fmt.Sprintf("/%s/election/%s", e.clusterName, e.shardName)
}
