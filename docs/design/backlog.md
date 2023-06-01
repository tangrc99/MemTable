# Backlog

Backlog 是 Redis 中用于主从复制功能的缓冲区，客户端的命令会先缓存在 Backlog 中，等待定时时间触发，批量地异步发送给从节点。在 Backlog 中，如果缓冲区写满，那么新内容将会自动覆盖旧内容，这非常适合使用 ring_buffer 结构来实现。

ring_buffer 是一个基于数组和写标识位实现的环形缓冲区，在`MemTable`中结构体定义如下：

```go
type RingBuffer struct {
	buffer   []byte	// 缓冲区
	offset   uint64	// 写标识位
	capacity uint64	// 容量
}
```

结构体的原理非常简单，即`offset`超过`capacity`时，进行取余的操作实现回环的机制。由于`offset`是单调递增的，所以在一次主从复制周期时，`offset`的位置就对应着`backlog_offset`的位置，`offset-capacity`就对应着 Backlog 中存在的最小偏移量。ring_buffer 会自动覆盖旧缓冲区的这种特性，能够衡量从节点的落后程度。如果某一从节点的复制偏移量小于最小偏移量，就意味着该节点落后太多，需要再次全量复制进行同步。

ring_buffer 的一个经典实现是 Linux kernel 中的`kfifo`结构。`kfifo`针对基础的 ring_buffer 做出了一些优化，如取余操作、单生产者-消费者场景下的无锁并发读写。由于 Backlog 本身并不涉及并发问题，因此`MemTable`中只针对取余操作进行了优化。该优化的原理其实也比较简单——保证 ring_buffer 的长度为 2 的幂，然后通过位运算的方式来替代取余操作。

```go
# 两个操作是等价的
insertPos := b.offset % b.capacity
insertPos := b.offset & (b.capacity - 1)
```

这是因为 2 次幂在减去 1 后，所有的低位上都是 1。如 capacity = 8，即二进制中的`1000`，那么 capacity - 1 = `0111`，所以这两个操作是等价的。

### 动态扩容

与 AOF 相同，backlog 同样也面临着“大日志”的情况，即一条日志的长度超出了 ring_buffer 的缓冲区容量；但 ring_buffer 结构体很难像 link_buffer 一样使用追加链表的方式来临时存储“大日志”。`MemTable`中使用的是动态扩容的方式来处理“大日志”，当单次写入的字符串长度大于 ring_buffer 的容量时，ring_buffer 会将当前缓冲区内的可读区域复制到另一片新开辟的内存上，并在新内存区域中写入“大日志”。