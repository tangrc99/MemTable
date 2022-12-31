package structure

import (
	"math/rand"
)

type skipListNode struct {
	next   []*skipListNode
	height int
	key    float32
	value  any
}

func newSkipListNode(key float32, value any, height int) *skipListNode {
	return &skipListNode{
		next:   make([]*skipListNode, height),
		height: height,
		key:    key,
		value:  value,
	}
}

func (node *skipListNode) getNextNode(level int) *skipListNode {
	return node.next[level]
}

func (node *skipListNode) changeNextNode(level int, next *skipListNode) {
	old := node.next[level]
	node.next[level] = next
	next.next[level] = old
}

func (node *skipListNode) removeNextNode(level int) {
	new_ := node.next[level].next[level]
	node.next[level] = new_
}

type SkipList struct {
	size  int
	level int
	head  *skipListNode
}

func NewSkipList(level int) *SkipList {
	return &SkipList{
		size:  0,
		level: level,
		head:  newSkipListNode(-1, "", level),
	}
}

func randomHeight(max int) int {
	level := 1
	for rand.Int()%2 != 0 {
		level++
	}
	if level > max {
		return max
	}
	return level
}

func (sl *SkipList) Insert(key float32, value any) {

	// 需要找到每一个层次的前驱
	prevs := make([]*skipListNode, sl.level)
	cur := sl.head

	// 每一个 prev 的 key 小于等于需要插入的 key
	for i := sl.level - 1; i >= 0; i-- {
		for nxt := cur.getNextNode(i); nxt != nil && nxt.key <= key; nxt = cur.getNextNode(i) {
			cur = nxt
		}
		prevs[i] = cur
	}

	// 允许重复，所以不加这一段
	//if prevs[0].key == key {
	//	prevs[0].value = value
	//	return
	//}

	// 随机生成高度节点
	height := randomHeight(sl.level)
	node := newSkipListNode(key, value, height)

	// 从底层到高层依次插入
	for i := 0; i < height; i++ {
		prevs[i].changeNextNode(i, node)
	}
	sl.size++

}

func (sl *SkipList) InsertIfNotExist(key float32, value any) bool {

	// 需要找到每一个层次的前驱
	prevs := make([]*skipListNode, sl.level)
	cur := sl.head
	// 每一个 prev 的 key 小于等于需要插入的 key
	for i := sl.level - 1; i >= 0; i-- {
		for nxt := cur.getNextNode(i); nxt != nil && nxt.key <= key; nxt = cur.getNextNode(i) {
			cur = nxt
		}
		prevs[i] = cur
	}

	// 如果前驱 key 相同则判断插入失败
	if prevs[0].key == key {
		return false
	}

	// 随机生成高度节点
	height := randomHeight(sl.level)
	node := newSkipListNode(key, value, height)

	// 从底层到高层依次插入
	for i := 0; i < height; i++ {
		prevs[i].changeNextNode(i, node)
	}
	sl.size++
	return true
}

func (sl *SkipList) Update(key float32, value any) bool {
	// 需要找到每一个层次的前驱
	cur := sl.head

	// 每一个 prev 的 key 小于等于需要插入的 key
	for i := sl.level - 1; i >= 0 && cur.key < key; i-- {
		for nxt := cur.getNextNode(i); nxt != nil && nxt.key <= key; nxt = cur.getNextNode(i) {
			cur = nxt
		}
	}

	if cur.key == key {
		cur.value = value
		return true
	}
	return false
}

func (sl *SkipList) Get(key float32) (any, bool) {
	// 需要找到每一个层次的前驱
	cur := sl.head

	// 每一个 prev 的 key 小于等于需要插入的 key
	for i := sl.level - 1; i >= 0 && cur.key < key; i-- {
		for nxt := cur.getNextNode(i); nxt != nil && nxt.key <= key; nxt = cur.getNextNode(i) {
			cur = nxt
		}
	}

	if cur.key == key {
		return cur.value, true
	}
	return nil, false
}

func (sl *SkipList) Delete(key float32) bool {

	// 需要找到每一个层次的前驱
	prevs := make([]*skipListNode, sl.level)
	cur := sl.head

	// 每一个 prev 的 key 小于需要插入的 key，然后判断下一个键
	for i := sl.level - 1; i >= 0; i-- {
		for nxt := cur.getNextNode(i); nxt != nil && nxt.key < key; nxt = cur.getNextNode(i) {
			cur = nxt
		}
		prevs[i] = cur
	}

	height := 0
	if prevs[0].getNextNode(0) != nil && prevs[0].getNextNode(0).key == key {
		height = prevs[0].getNextNode(0).height

		// 从底层到高层依次插入
		for i := 0; i < height; i++ {
			prevs[i].removeNextNode(i)
		}
		sl.size--
		return true

	}
	return false
}

func (sl *SkipList) Exist(key float32) bool {
	// 需要找到每一个层次的前驱
	cur := sl.head

	// 每一个 prev 的 key 小于等于需要插入的 key
	for i := sl.level - 1; i >= 0 && cur.key < key; i-- {
		for nxt := cur.getNextNode(i); nxt != nil && nxt.key <= key; nxt = cur.getNextNode(i) {
			cur = nxt
		}
	}

	return cur.key == key
}

func (sl *SkipList) Size() int {
	return sl.size
}

func (sl *SkipList) Range(min, max float32) ([]any, int) {

	// 需要找到每一个层次的前驱
	cur := sl.head

	// 每一个 prev 的 key 小于等于需要插入的 key
	for i := sl.level - 1; i >= 0 && cur.key < min; i-- {
		for nxt := cur.getNextNode(i); nxt != nil && nxt.key <= min; nxt = cur.getNextNode(i) {
			cur = nxt
		}

	}

	for ; cur != nil && cur.key < min; cur = cur.getNextNode(0) {
	}

	values := make([]any, 0)
	size := 0

	for ; cur != nil && cur.key <= max; cur = cur.getNextNode(0) {
		values = append(values, cur.value.(string))
		size++
	}

	return values, size
}

func (sl *SkipList) CountByRange(min, max float32) int {
	// 需要找到每一个层次的前驱
	cur := sl.head

	// 每一个 prev 的 key 小于等于需要插入的 key
	for i := sl.level - 1; i >= 0 && cur.key < min; i-- {
		for nxt := cur.getNextNode(i); nxt != nil && nxt.key <= min; nxt = cur.getNextNode(i) {
			cur = nxt
		}

	}

	for ; cur != nil && cur.key < min; cur = cur.getNextNode(0) {
	}

	size := 0

	for ; cur != nil && cur.key <= max; cur = cur.getNextNode(0) {
		size++
	}

	return size
}

func (sl *SkipList) GetPosByKey(key float32) int {
	pos := -1
	cur := sl.head
	for ; cur != nil && cur.key <= key; cur = cur.getNextNode(0) {
		pos++
	}
	if cur == nil || cur.key != key {
		return -1
	}
	return pos
}

func (sl *SkipList) DeleteRange(min, max float32) ([]any, int) {
	// 需要找到每一个层次的前驱
	cur := sl.head

	deleted := 0
	values := make([]any, 0)

	// 每一个 prev 的 key 小于等于需要插入的 key
	for i := sl.level - 1; i >= 0 && cur.key < min; i-- {

		for nxt := cur.getNextNode(i); nxt != nil && nxt.key < min; nxt = cur.getNextNode(i) {
			cur = nxt
		}
		// 这里 cur 的值正好是 < min 的
		n := cur.getNextNode(i)
		for ; n != nil && n.key <= max; n = n.getNextNode(i) {
			if i == 0 {
				values = append(values, n.value)
				deleted++
			}

		}
		// 这里 n 的值 > max
		cur.next[i] = n
	}
	sl.size -= deleted
	return values, deleted
}

func (sl *SkipList) DeletePos(start, end int) ([]any, int) {
	//fixme : 性能较低，一次遍历

	// 判别位置
	if start < 0 {
		start += sl.size
	}
	if end < 0 {
		end += sl.size
	}
	if start > end || end < 0 || start >= sl.size {
		return nil, 0
	}
	if start < 0 {
		start = 0
	}
	if end >= sl.size {
		end = sl.size - 1
	}

	cur := sl.head

	for i := 0; i < start; i++ {
		cur = cur.getNextNode(0)
	}

	min := cur.getNextNode(0).key
	for i := start; i < end; i++ {
		cur = cur.getNextNode(0)
	}
	max := cur.getNextNode(0).key
	return sl.DeleteRange(min, max)

}

func (sl *SkipList) Pos(start, end int) ([]any, int) {

	// 判别位置
	if start < 0 {
		start += sl.size
	}
	if end < 0 {
		end += sl.size
	}
	if start > end || end < 0 || start >= sl.size {
		return nil, 0
	}
	if start < 0 {
		start = 0
	}
	if end >= sl.size {
		end = sl.size - 1
	}

	cur := sl.head

	for i := 0; i <= start; i++ {
		cur = cur.getNextNode(0)
	}

	values := make([]any, 0)

	for i := start; i <= end; i++ {
		values = append(values, cur.value)
		cur = cur.getNextNode(0)
	}
	return values, end - start + 1

}
