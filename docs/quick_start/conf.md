# Config

目前配置文件所允许的全部配置如下：

```
# 监听宿主机名
host 127.0.0.1
# 监听端口号，设置为 0 代表关闭 tcp 端口
port 6380

# 日志等级，可选项：debug, info, warning, error, panic
loglevel info
# 数据库数量
databases 16
# 工作目录
dir ./
# 是否开启 aof 持久化
appendonly true
# 以守护进程模式启动
daemonize false
# rdb 持久化文件名
dbfilename dump.rdb

# 客户端过期时间 <秒>，-1 代表无过期时间
timeout 300
# 最大客户端数量，-1 代表不开启
maxclients 10000
# 最大内存 <bytes>，-1 代表不开启
maxmemory -1
# 驱逐策略 lru lfu no
eviction no

# 是否开启协程池，用于客户端请求处理与报文解析
gopool true
# 协程池最大协程数量，该参数决定了最大连接数
# 一个客户端连接会占用两个协程
# 客户端最大数量 <= gopoolsize/2
gopoolsize 10000
# 协程池初始协程数量
gopoolspawn 200

# tls 端口号，设置为 0 代表关闭 tls 端口
tls-port 0
# 是否要求客户端证书校检
tls-auth-clients true
# 服务端私钥
tls-key-file ./tests/tls/redis.key
# 服务端证书
tls-cert-file ./tests/tls/redis.crt
# 根证书
tls-ca-cert-file ./tests/tls/ca.crt

# 是否以集群模式运行，该功能尚不完善
clusterenable false
# 集群名称
clustername cluster_000

# 慢查询日志阈值 ms
slowlog-log-slower-than 1000
# 慢查询日志最大记录数
slowlog-max-len 100

# 访问控制列表配置文件
aclfile conf/users.acl
```