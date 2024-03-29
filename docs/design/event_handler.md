# Event Handler

`MemTable` 中使用一个 Event Loop 来处理所有事件，Event Loop 的核心是一个 select 语句：

```go
for !quit {
	select {
	case io_event:
		// handle client request.
	case time_event:
        // handle time event.
    default:
    	// do something      
	}
}
```

select 语句的执行逻辑与 Redis Event Loop 的执行逻辑是相同的：每一次循环选择一个已经就绪的客户端请求处理，或选择已经触发的时间事件处理，如果当前没有任何客户端请求与时间事件，事务循环会做一些其他的必须工作。

## 客户端请求处理

`MemTable`中客户端请求部分不存在并发竞争，客户端请求在 Event Loop 中的处理流程如下：

1. 更新客户端状态；
2. 执行客户端请求命令；
3. 根据执行时间，记录慢查询日志；
4. 如果是执行成功的写操作，写入 AOF 日志；
5. 处理结果返回 IO 线程，归还对象池。

## 单事务线程的优缺点

单事务线程的优点比较明显，即内存数据库操作部分不存在并发竞争，开发服务器的功能更为简单，能够实现更多的功能。但是相应地，单事务线程在性能上**可能**会较低。如果应用于写少读多的场景，多事务线程的设计能够使用读写锁实现低成本的并发，读性能显然会更高。但是在写多读少或读写均匀的场景下，多事务线程的读写锁性能会下降较为严重，并且还需要在 AOF、backlog 等模块处理并发问题，整个程序的临界区会增多，性能提升就会比较有限。另一方面，在 pprof 测试中，`MemTable`以及其他类似项目的性能瓶颈点其实并不在事务操作上，而是集中在 goroutine 调度以及网络 IO 上，使用多线程带来的性能提升并不明显。
