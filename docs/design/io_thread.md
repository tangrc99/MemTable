# IO Thread

IO 线程负责网络 IO 读写、 RESP 报文解析、处理阻塞事件，IO 线程之间不存在临界区问题。IO 线程的核心是一个 select 多路复用：

```go
select {
case req_msg:
	// 处理解析完毕的 RESP 报文
	...
case echo_msg:
	// 处理回复消息
	...
case block_msg:
	// 处理阻塞消息
	...
}
```

分支 1 的主要逻辑是检查 TCP 连接是否正常、RESP 报文解析是否正常，如果出现无法恢复的错误则关闭客户端连接；如果一切正常，将解析完毕的报文通过 channel 发送给 Event Loop 处理。分支 2 的主要逻辑是将 Event Loop 的执行结果写回到 Socket 中。分支 3 的主要逻辑是等待 pub/sub、brpop 等阻塞请求的结果，将结果写回到 Socket 中。

分支 1 与分支 2 之间并不是完全串行的，这种设计能够提升 pipeline 模式下的请求吞吐量，但是在 CPU 核心较少的情况下，可能会导致 goroutine 调度的性能下降。
