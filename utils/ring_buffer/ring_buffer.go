package ring_buffer

// RingBuffer 维护一个环形缓冲区，非线程安全
type RingBuffer struct {
	buffer   []byte
	offset   uint64
	capacity uint64
}

// Init 将 RingBuffer 缓冲区大小初始化为 2^capacity
func (b *RingBuffer) Init(capacity uint64) {
	if capacity&(capacity-1) != 0 {
		capacity |= capacity >> 1
		capacity |= capacity >> 2
		capacity |= capacity >> 4
		capacity |= capacity >> 8
		capacity |= capacity >> 16
		capacity += 1
	}
	b.capacity = capacity
	b.buffer = make([]byte, capacity, capacity)
	b.offset = 0
}

// LowWaterLevel 返回环形缓冲区中保留的最小序列号
func (b *RingBuffer) LowWaterLevel() uint64 {
	if b.offset < b.capacity {
		return 0
	}
	return b.offset - b.capacity
}

// HighWaterLevel 返回环形缓冲区中保留的最大序列号
func (b *RingBuffer) HighWaterLevel() uint64 {
	return b.offset
}

// Append 将内容拷贝到 RingBuffer 中，并返回拷贝后的 offset
func (b *RingBuffer) Append(content []byte) uint64 {

	// 如果内容过大，需要进行动态扩充
	if uint64(len(content)) >= b.capacity {

		// 读取旧值
		oldContent := b.ReadSince(b.LowWaterLevel())
		// 计算所需大小，并且向上取整
		b.Init(uint64(len(content)) + uint64(len(oldContent)))

		b.Append(oldContent)
		b.Append(content)
		return uint64(len(content))
	}

	Len := uint64(len(content))

	insertPos := b.offset & (b.capacity - 1)
	size := b.capacity - insertPos
	if size < Len {
		copy(b.buffer[insertPos:], content[0:size])
		copy(b.buffer[0:], content[size:])
	} else {
		copy(b.buffer[insertPos:], content)
	}

	b.offset += Len

	return b.offset
}

// Read 从 offset 开始读取最大 max 大小的内容，如果 offset 小于 LowWaterLevel 或大于 HighWaterLevel，返回[]byte{}
func (b *RingBuffer) Read(offset, max uint64) []byte {

	if offset < b.LowWaterLevel() || offset >= b.HighWaterLevel() {
		return []byte{}
	}
	if offset+max > b.HighWaterLevel() {
		max = b.offset - offset
	}

	content := make([]byte, max)

	readPos := offset & (b.capacity - 1)
	size := b.capacity - readPos

	if max > size {
		ll := max - size
		copy(content[0:], b.buffer[readPos:])
		copy(content[size:], b.buffer[0:ll])

	} else {
		copy(content[0:], b.buffer[readPos:max+readPos])
	}

	return content
}

// ReadSince 从 offset 开始读取缓冲区所有内容，如果 offset 小于 LowWaterLevel 或大于 HighWaterLevel，返回[]byte{}
func (b *RingBuffer) ReadSince(offset uint64) []byte {

	if offset < b.LowWaterLevel() || offset >= b.HighWaterLevel() {
		return []byte{}
	}

	Len := b.offset - offset
	content := make([]byte, Len)

	readPos := offset & (b.capacity - 1)
	size := b.capacity - readPos

	if Len > size {
		ll := Len - size
		copy(content[0:], b.buffer[readPos:])
		copy(content[size:], b.buffer[0:ll])

	} else {
		copy(content, b.buffer[readPos:Len+readPos])
	}

	return content
}

// Capacity 返回 ring buffer 的最大容量
func (b *RingBuffer) Capacity() uint64 {
	return b.capacity
}
