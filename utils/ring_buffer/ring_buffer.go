package ring_buffer

// RingBuffer 维护一个环装缓冲区
type RingBuffer struct {
	buffer   []byte
	offset   uint64
	capacity uint64
}

func (b *RingBuffer) Init(capacity uint64) {
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

func (b *RingBuffer) Append(content []byte) uint64 {

	if uint64(len(content)) >= b.capacity {
		panic("Content Length is Larger Than Capacity")
	}

	Len := uint64(len(content))

	insertPos := b.offset % b.capacity
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

func (b *RingBuffer) Read(offset, max uint64) []byte {

	if offset < b.LowWaterLevel() || offset >= b.HighWaterLevel() {
		return []byte{}
	}
	if offset+max > b.HighWaterLevel() {
		max = b.offset - offset
	}

	content := make([]byte, max)

	readPos := offset % b.capacity
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

func (b *RingBuffer) ReadSince(offset uint64) []byte {

	if offset < b.LowWaterLevel() || offset >= b.HighWaterLevel() {
		return []byte{}
	}

	Len := b.offset - offset
	content := make([]byte, Len)

	readPos := offset % b.capacity
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
