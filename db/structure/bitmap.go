package structure

// BitMap 提供了 bit 层级的操作
type BitMap []byte

// NewBitMap 创建一个空的 BitMap
func NewBitMap(cap int) *BitMap {

	if cap%8 != 0 {
		cap += 8 - cap%8
	}

	a := make([]byte, cap/8)
	return (*BitMap)(&a)
}

// NewBitMapFromBytes 使用指定 bytes 创建 BitMap
func NewBitMapFromBytes(bytes []byte) *BitMap {
	a := BitMap(bytes)
	return &a
}

// ByteLen 返回 BitMap 的字节大小
func (b *BitMap) ByteLen() int {
	return len(*b)
}

// Get 获取指定位置上的 bit 值
func (b *BitMap) Get(pos int) byte {
	// 第几个 byte
	byteSeq := pos / 8
	// byte 中的第几个 bit
	bitSeq := pos % 8

	if byteSeq >= len(*b) {
		return 0
	}

	return ((*b)[byteSeq] >> bitSeq) & 0x01
}

// Set 修改指定位置上的 bit 值
func (b *BitMap) Set(pos int, val byte) {

	// 第几个 byte
	byteSeq := pos / 8
	// byte 中的第几个 bit
	bitSeq := pos % 8

	// 如果大小不够，需要生长
	if space := byteSeq - len(*b); space >= 0 {
		*b = append(*b, make([]byte, space+1)...)
	}

	if val == 1 {
		(*b)[byteSeq] |= byte(1 << bitSeq)
	} else {
		(*b)[byteSeq] &^= byte(1 << bitSeq)
	}
}

// GetSet 修改指定位置上的 bit 值，并返回旧值
func (b *BitMap) GetSet(pos int, val byte) byte {
	old := b.Get(pos)
	b.Set(pos, val)
	return old
}

// Count 返回 byte 范围内 bit 值为 1 的 bit 数量； start 和 end 都是 byte 的位置，而不是 bit 位置
func (b *BitMap) Count(start, end int) int {
	// 位置检查
	maxLen := b.ByteLen()
	if start < 0 {
		start += maxLen
	}
	if end < 0 {
		end += maxLen
	}

	if start > end || end < 0 || start >= maxLen {
		return 0
	}
	if start < 0 {
		start = 0
	}
	if end >= maxLen {
		end = maxLen - 1
	}

	count := 0
	for _, byteVal := range (*b)[start : end+1] {
		for ; byteVal != 0x00; byteVal >>= 1 {
			if byteVal&0x01 == 0x01 {
				count++
			}
		}
	}

	return count
}

// Pos 返回 byte 范围内 bit 值为 val 的起始位置； start 和 end 都是 byte 的位置，而不是 bit 位置
func (b *BitMap) Pos(val byte, start, end int) int {
	// 位置检查
	maxLen := b.ByteLen()
	if start < 0 {
		start += maxLen
	}
	if end < 0 {
		end += maxLen
	}

	if start > end || end < 0 || start >= maxLen {
		return -1
	}
	if start < 0 {
		start = 0
	}
	if end >= maxLen {
		end = maxLen - 1
	}

	pos := start * 8

	for _, byteVal := range (*b)[start : end+1] {

		for i := 7; i >= 0; i-- {

			if byteVal>>i != val {

				pos++

			} else {
				return pos
			}
		}
	}
	return -1
}

func (b *BitMap) RangeSet(val byte, start, end int) {
	maxLen := b.ByteLen() * 8
	if start < 0 {
		start += maxLen
	}
	if end < 0 {
		end += maxLen
	}

	if start > end || end < 0 || start >= maxLen {
		return
	}
	if start < 0 {
		start = 0
	}
	if end >= maxLen {
		end = maxLen - 1
	}

	for i := start; i <= end; {
		if i%8 == 0 && end-i >= 8 {
			if val == 1 {
				(*b)[i/8] = 255
			} else {
				(*b)[i/8] = 0
			}
			i += 8
		} else {
			b.Set(i, val)
			i++
		}
	}
}
