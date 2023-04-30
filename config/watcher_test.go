package config

import (
	"os"
	"testing"
)

func TestWatcher(t *testing.T) {

	file := "test.conf"

	tmpFile, err := os.Create(file)
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() {
		err = os.Remove(file)
	})

	Conf.ConfFile = file
	confWatcherEnabled = true
	initWatcher()

	_, _ = tmpFile.WriteString("# 监听宿主机名\nhost 127.0.0.1\n\n# 监听端口号\nport 6382\n\n# tls 端口号\ntls-port 0\n# 是否要求客户端证书\ntls-auth-clients true\n# 服务端私钥\ntls-key-file ./tests/tls/redis.key\n# 服务端证书\ntls-cert-file ./tests/tls/redis.crt\n# 根证书\ntls-ca-cert-file ./tests/tls/ca.crt\n\n# 日志等级\nloglevel info\n\n# 数据库数量\ndatabases 16\n\n# 客户端过期时间\ntimeout 300\n\n# 工作目录\ndir ./\n\n# 最大客户端数量，-1 代表不开启\n# maxclients 10000\n\n# 最大内存，-1 代表不开启\n# maxmemory <bytes>\n\n# 驱逐策略 lru lfu no\neviction no\n\n# 是否开启 aof\nappendonly true\n\n# 是否开启协程池，用于客户端请求处理\ngopool true\n\n# 协程池最大协程数量，该参数决定了最大连接数\n# 客户端最大数量 = gopoolsize/2\ngopoolsize 10000\n\n# 协程池空转数量\ngopoolspawn 200\n\n# rdb 持久化文件名\ndbfilename dump.rdb\n\nclusterenable false\n\nclustername cluster_000\n\n# 是否在后台运行\ndaemonize false\n\nslowlog-log-slower-than 1000\nslowlog-max-len 100\n\n\naclfile conf/users.acl")

	e := <-ConfWatcher.Notification()

	for _, f := range e.Fields {
		println(f)
	}
}
