package structure

type BitMap []byte

func NewBitMap() *BitMap {
	a := make([]byte, 0)
	return (*BitMap)(&a)
}

func NewBitMapFromBytes(bytes []byte) *BitMap {
	a := BitMap(bytes)
	return &a
}

func (b *BitMap) ByteLen() int {
	return len(*b)
}

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

func (b *BitMap) GetSet(pos int, val byte) byte {
	old := b.Get(pos)
	b.Set(pos, val)
	return old
}

// Count 中 start 和 end 都是 byte 的位置，而不是 bit 位置
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

//
//func (b *BitMap)
