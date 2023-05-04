# MemTable

MemTable 是一个仿照 Redis 架构写成的，基于内存的键值对存储服务，使用 RESP 协议进行通信。本项目是一个完全基于 Golang 的玩具项目，仅用于学习。

## Features

- 支持 redis 客户端和 RESP 通信协议，支持 redis pipeline 通信；
- 支持 String,List,Set,ZSet,Hash,Bitmap 等多种数据结构；
- 支持 pub/sub，基于前缀树实现路径递归发布；
- 支持 TTL 功能，可以设置键值对过期；
- 支持 AOF、RDB 持久化；
- 支持 Lua 脚本扩展；
- 支持 ACL 控制；
- 支持主从复制；
- 支持分片集群，暂时不支持自动故障恢复；
