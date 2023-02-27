package server

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/tangrc99/MemTable/config"
	"github.com/tangrc99/MemTable/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"strings"
	"time"
)

const announceInterval = 10

type etcdWatcher struct {
	clusterName string // 集群名称
	shard       int    // 当前节点 shard
	shardName   string // 当前节点 shard 名称
	host        string // 当前节点 host

	called uint64 // 用于控制 announce 间隔

	ele *concurrency.Election
	cli *clientv3.Client

	clusterWatcher
}

func initEtcdWatcher(clusterName string, host string) *etcdWatcher {

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"10.0.0.124:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logger.Error("Cluster etcd connection:", err.Error())
	}

	return &etcdWatcher{
		clusterName: clusterName,
		cli:         cli,
		host:        host,
	}
}

func (e *etcdWatcher) getClusterConfig() clusterConfig {

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

	if valid, reason := ccfg.isValid(); !valid {
		logger.Panic("Cluster Invalid Config", reason)
	}

	return ccfg
}

// initCampaign 如果失败，可能是由于该节点掉线后重连；这时候需要自动降级为副节点。
func (e *etcdWatcher) initCampaign(shardNum int, isMaster bool) bool {

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

func (e *etcdWatcher) watchClusterChanges() <-chan clusterChangeMessage {

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
func (e *etcdWatcher) whoIsMaster() string {

	ret, err := e.ele.Leader(context.TODO())
	if err != nil {
		logger.Error("Cluster No Master")
		return ""
	}
	leader := string(ret.Kvs[0].Value)
	return leader
}

func (e *etcdWatcher) campaign() bool {

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

func (e *etcdWatcher) leaderAnnounce(downNodes []string) {

	if e.called%announceInterval != 0 {
		return
	}

	pCh := e.publishChannel()

	tx := e.cli.Txn(context.TODO())
	msg1 := generateAnnounceMessage(e.shard, e.host)
	msg2 := generateNodeDownMessage(e.shard, strings.Join(downNodes, ","))

	tx.Then(clientv3.OpPut(pCh, msg1))
	if msg2 != "" {
		tx.Then(clientv3.OpPut(pCh, msg2))
	}

	ret, err := tx.Commit()
	if err != nil || !ret.Succeeded {
		logger.Error("Cluster Publish Message Error, Info", err.Error())
		e.called++
	}

	e.called++
}

func (e *etcdWatcher) downNodeAnnounce(nodes []string) {
	// 如果成功了，需要在广播 channel 告知全部节点
	pCh := e.publishChannel()

	msg := generateNodeDownMessage(e.shard, strings.Join(nodes, ","))

	_, err := e.cli.Put(context.TODO(), pCh, msg)
	if err != nil {
		logger.Error("Cluster Publish Message Error, Info", err.Error())
	}
}

func (e *etcdWatcher) upNodeAnnounce(node string) {
	// 如果成功了，需要在广播 channel 告知全部节点
	pCh := e.publishChannel()

	msg := generateNodeUpMessage(e.shard, node)

	_, err := e.cli.Put(context.TODO(), pCh, msg)
	if err != nil {
		logger.Error("Cluster Publish Message Error, Info", err.Error())
	}
}

/* ---------------------------------------------------------------------------
* utils 函数
* ------------------------------------------------------------------------- */

func (e *etcdWatcher) publishChannel() string {
	return fmt.Sprintf("/%s/channel", e.clusterName)
}

func (e *etcdWatcher) electionChannel() string {
	return fmt.Sprintf("/%s/election/%s", e.clusterName, e.shardName)
}
