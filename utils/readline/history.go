package readline

import (
	"bytes"
	"container/list"
)

// history 是一个历史命令链表，支持命令查询功能，查询的时间复杂度是 O(n)。
type history struct {
	limit       int           // 存储上限
	sentry      *list.Element // 链表哨兵
	commands    *list.List    // 历史命令链表
	searchCache []byte        // 查询命令缓存
	cursor      *list.Element // 查询命令缓存
}

// newHistory 创建一个 history 对象，存储上限为 limit
func newHistory(limit int) *history {
	l := list.New()
	l.PushFront([]byte{})
	return &history{
		limit:    limit,
		commands: l,
		sentry:   l.Front(),
	}
}

// setLimitation 重新设置 limit 参数
func (h *history) setLimitation(limit int) {
	h.limit = limit
	for h.commands.Len()-1 > h.limit {
		h.commands.Remove(h.commands.Back())
	}
}

// recordCommand 用于追加命令
func (h *history) recordCommand(command []byte) {
	h.commands.InsertAfter(command, h.sentry)
	if h.commands.Len()-1 > h.limit {
		h.commands.Remove(h.commands.Back())
	}
	h.resetCursor()
}

// searchCommand 查询命令
func (h *history) searchCommand(sub []byte) []byte {

	if h.commands.Len() <= 1 {
		return []byte{}
	}

	if !bytes.Equal(sub, h.searchCache) {
		h.searchCache = sub
		h.resetCursor()
	}
	for ; h.cursor != nil; h.cursor = h.cursor.Next() {
		v := h.cursor.Value.([]byte)
		if matched := bytes.Contains(v, sub); matched {
			h.cursor = h.cursor.Next()
			return v
		}
	}
	return []byte{}
}

// moveCursor 执行一次查询游标的移动。如果游标无法移动，返回值 end == true
func (h *history) moveCursor(older bool) (command []byte, end bool) {

	if h.commands.Len() <= 1 {
		return []byte{}, true
	}

	if h.cursor == nil {
		return []byte{}, true
	}

	if older {
		if h.cursor.Next() != nil {
			h.cursor = h.cursor.Next()
			return h.cursor.Value.([]byte), false
		} else {
			return []byte{}, true
		}
	}

	if h.cursor.Prev() == nil {
		return []byte{}, true
	} else {
		h.cursor = h.cursor.Prev()
		return h.cursor.Value.([]byte), false
	}
}

// resetCursor 重置游标的位置
func (h *history) resetCursor() {
	h.cursor = h.sentry
}

// clean 清理已经缓存的命令
func (h *history) clean() {
	l := list.New()
	l.PushFront([]byte{})

	h.commands = l
	h.sentry = l.Front()
	h.cursor = h.sentry
	h.searchCache = []byte{}
}

// histories 获取所有的历史命令
func (h *history) histories() [][]byte {
	var histories [][]byte
	for c := h.sentry.Next(); c != nil; c = c.Next() {
		histories = append(histories, c.Value.([]byte))
	}
	return histories
}
