package structure

// 这里借鉴 etcd 的设计，使用两种数据结构来实现发布订阅频道，允许使用目录来进行匹配

type Channel struct {
}
