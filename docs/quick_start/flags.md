# Flags

使用 --flags 来启动`memtable`与`memtable-cli`。

## Example

以 --flags 来启动`memtable`：

```shell
# 使用配置文件启动
memtable --conf default.conf

# 以指定 host 和 port 启动
memtable --host 127.0.0.1 --port 6380

# 只允许 tls 端口访问
memtable --tls-port 6380 --port 0

# 以守护进程方式启动
memtable --daemonize

# 打印帮助文档
memtable --help
```

以 --flags 来启动`memtable-cli`：

```shell
# 连接指定 host 与 port
memtable-cli --host 127.0.0.1 --port 6380

# 以非交互模式执行
memtable-cli get key

# 从标准输入读取参数
memtable-cli -x get

# 打印帮助文档
memtable-cli --help
```

