# AppendOnly File
AOF 是 Redis 中的追加日志，开启 AOF 功能时，客户端的命令会被复制到 AOF 缓冲区中，随后按照一定的策略写入到硬盘中。在 `MemTable`中，AOF 缓冲区并没有借鉴 Redis 中的实现，而是以 link_buffer 的形式组织的。link_buffer 即将多个缓冲区以链表的形式组织起来，使用读和写两个标识位记录读写位置。由于 link_buffer 具有单生产者-消费者情景下的无锁并发读写特性，经常被应用在异步日志、网络缓冲区等场景下，如 InnoDB 的日志缓冲区、NetPoll 中的网络缓冲区。

## 异步刷盘

如果基于链表而非循环链表的形式来组织 link_buffer，那么日志缓冲区能够很轻松地利用其无锁读写的特性实现异步日志刷盘。这种情况下，不需要考虑缓冲区写满时的并发竞争问题。但以这种方式组织日志缓冲区可能会导致更为频繁的内存申请操作，在密集写的情况下性能较差。一个比较通用的优化策划是使用一个日志缓冲区内存池来进行内存管理。

`MemTable`并没有使用链表形式，而是使用循环链表的形式来组织 link_buffer，这种设计需要考虑缓冲区写满时的并发竞争问题。当 AOF 缓冲区写满时，必须采取一定的策略来清空缓冲区。一般情况下，采取的策略是：当缓冲区写满时，缓冲区的写入者需要控制完成一次刷盘后，继续写入日志缓冲区。这种策略的好处是能够调节程序的写入速度；当缓冲区写满时，意味着当前的写入速度已经超过硬盘能够承载的范围，使用写入线程主动刷盘能够有效地调节程序的写入速度。

使用写入线程进行刷盘，需要考虑到负责刷盘的后台线程是否正在写入。如果后台线程正在进行刷盘动作，必须要停止等待，防止日志乱序。`MemTable`中使用一个原子标志位来解决这种并发竞争问题。

```go
func (buff *aofBuffer) asyncTask() {
    ...
    // 自旋等待进入临界区
    for !atomic.CompareAndSwapInt32(&buff.writing, 0, 1) {
    }
    buff.flushBuffer()

    // os 缓冲区写入硬盘
    atomic.StoreInt32(&buff.writing, 2)
    buff.syncToDisk()

    // 完成刷盘工作
    atomic.StoreInt32(&buff.writing, 0)
    ...
}


func (buff *aofBuffer) append(bytes []byte) {
	...
    // 如果缓冲区已经写满
    if buff.appendSeq-buff.flushSeq == buff.pageSize {
        // 尝试进入临界区
        if atomic.CompareAndSwapInt32(&buff.writing, 0, 1) {
            // 成功进入临界区，刷盘腾出空间
            buff.flushBuffer()
            // 退出临界区
            atomic.StoreInt32(&buff.writing, 0)
        } else {
            // 进入临界区失败，自旋等待刷盘完成
            for atomic.LoadInt32(&buff.writing) != 0 {
            }
            // 加快写入频率
            buff.flush()
        }
    }
	...
}
```

在进入临界区前，写入线程需要通过 CAS 操作修改标识位，如果标识位修改失败，意味着当前后台线程正在进行刷盘动作，这时需要自旋等待刷盘完成。

## 大日志问题

大日志是指日志大小超过 link_buffer 单页缓冲区大小的日志。Redis 中的设计是为这些日志单独开辟缓冲区写入，`MemTable`中使用了类似的设计。`bufferPage`是 link_buffer 中的一页：

```go
// bufferPage 是一个单写缓冲区，如果需要多写需要加锁
type bufferPage struct {
	content  []byte   // page 内容
	pos      int      // 当前写入位置
	max      int      // page 最大值
	appendix [][]byte // 用于存储大日志
}
```

大日志不会被直接写入到 link_buffer 中，而是单独开辟缓冲区并写入`appendix`字段。当`appendix`字段被写入后，该页将不被允许继续写入。

另一种做法是将大日志拆分为多段，每一段写入 link_buffer 的一页。这种做法的好处是不需要额外分配内存，坏处是如果 link_buffer 的页数设置过少，可能会需要多次刷盘才能完成大日志的写入，这会导致写入线程发生阻塞。
