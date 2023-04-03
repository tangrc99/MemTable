package structure

// ListNode 是一个双向链表节点
type ListNode struct {
	next, prev *ListNode
	list       *List
	Value      Object
}

// Next 返回 ListNode 的下一个节点指针，若当前 ListNode 为链表尾，返回 nil
func (node *ListNode) Next() *ListNode {
	if node.next == node.list.head {
		return nil
	}
	return node.next
}

// Prev 返回 ListNode 的上一个节点指针，若当前 ListNode 为链表头，返回 nil
func (node *ListNode) Prev() *ListNode {
	if node.prev == node.list.head {
		return nil
	}
	return node.prev
}

// List 是一个双向链表
type List struct {
	head *ListNode
	size int
}

// NewList 创建一个双向链表并返回指针
func NewList() *List {
	l := List{
		head: nil,
		size: 0,
	}
	head := &ListNode{
		next: nil, prev: nil, list: &l, Value: nil,
	}
	head.prev = head
	head.next = head
	l.head = head
	return &l
}

// FrontNode 返回链表中首个结点的指针，若链表为空，返回 nil
func (list *List) FrontNode() *ListNode {
	if list.head.next == list.head {
		return nil
	}
	return list.head.next
}

// BackNode 返回链表中最后一个节点的指针，若链表为空，返回 nil
func (list *List) BackNode() *ListNode {
	if list.head.prev == list.head {
		return nil
	}
	return list.head.prev
}

// Front 返回链表第一个节点存储的值，如果不存在值会返回 nil
func (list *List) Front() Object {
	return list.head.next.Value
}

// Back 返回链表最后一个节点存储的值，如果不存在值会返回 nil
func (list *List) Back() Object {
	return list.head.prev.Value
}

// InsertAfterNode 创建一个 ListNode 对象，并插入到 at 之前
func (list *List) InsertAfterNode(value Object, at *ListNode) *ListNode {
	if at == nil {
		return nil
	}

	if at.list != list {
		return nil
	}

	next := at.next
	node := ListNode{
		next:  next,
		prev:  at,
		Value: value,
		list:  list,
	}
	next.prev = &node
	at.next = &node
	list.size++
	return &node
}

// InsertBeforeNode 创建一个 ListNode 对象，并插入到 at 之后
func (list *List) InsertBeforeNode(value Object, at *ListNode) *ListNode {
	if at == nil {
		return nil
	}

	if at.list != list {
		return nil
	}

	prev := at.prev
	node := ListNode{
		next:  at,
		prev:  prev,
		Value: value,
		list:  list,
	}
	prev.next = &node
	at.prev = &node
	list.size++
	return &node
}

// RemoveNode 删除 at 节点，并返回其存储的值
func (list *List) RemoveNode(at *ListNode) Object {

	if at.list != list {
		return nil
	}

	prev := at.prev
	next := at.next

	prev.next = next
	next.prev = prev

	at.next = nil
	at.prev = nil
	list.size--
	return at.Value
}

// PushFront 创建一个 ListNode 对象，并插入到链表头
func (list *List) PushFront(value Object) {
	list.InsertAfterNode(value, list.head)
}

// PushBack 创建一个 ListNode 对象，并插入到链表尾
func (list *List) PushBack(value Object) {
	list.InsertBeforeNode(value, list.head)
}

// PopFront 删除链表头，并返回值
func (list *List) PopFront() Object {
	if list.Size() == 0 {
		return nil
	}

	return list.RemoveNode(list.head.next)
}

// PopBack 删除链表尾，并返回值
func (list *List) PopBack() Object {
	if list.Size() == 0 {
		return nil
	}
	return list.RemoveNode(list.head.prev)
}

// Size 返回链表节点数量
func (list *List) Size() int {
	return list.size
}

// Nil 返回链表是否为空链表
func (list *List) Empty() bool {
	return list.size == 0
}

// InsertAfter 将元素插入到给定位置后面，如果 pos 小于 0，代表倒序位置，如 -1 代表链表尾
func (list *List) InsertAfter(value Object, pos int) bool {
	if pos >= list.Size() || pos < 0 {
		return false
	}

	// 倒序插入
	if list.Size()-pos < pos {

		p := list.BackNode()
		pos = list.Size() - pos - 1
		for i := 0; i < pos; i++ {
			p = p.prev
		}
		list.InsertAfterNode(value, p)
		return true
	}

	p := list.head.next

	for i := 0; i < pos; i++ {
		p = p.next
	}

	return nil != list.InsertAfterNode(value, p)
}

// InsertBefore 将元素插入到给定位置后面，如果 pos 小于 0，代表倒序位置，如 -1 代表链表尾
func (list *List) InsertBefore(value Object, pos int) bool {

	if pos >= list.Size() || pos < 0 {
		return false
	}

	// 倒序插入
	if list.Size()-pos < pos {

		p := list.BackNode()
		pos = list.Size() - pos - 1
		for i := 0; i < pos; i++ {
			p = p.prev
		}
		list.InsertBeforeNode(value, p)
		return true
	}

	p := list.head.next

	for i := 0; i < pos; i++ {
		p = p.next
	}

	return nil != list.InsertBeforeNode(value, p)

}

// Pos 返回指定位置的值，如果 pos 小于 0，代表倒序位置，如 -1 代表链表尾。如果位置不存在，返回 nil
func (list *List) Pos(pos int) (Object, bool) {
	if pos < 0 {
		pos += list.Size()
	}

	if pos >= list.Size() || pos < 0 {
		return nil, false
	}

	// 倒序插入
	if list.Size()-pos < pos {

		p := list.BackNode()
		pos = list.Size() - pos - 1
		for i := 0; i < pos; i++ {
			p = p.prev
		}
		return p.Value, true
	}

	p := list.head.next

	for i := 0; i < pos; i++ {
		p = p.next
	}

	return p.Value, true
}

// PosNode 返回指定位置的 ListNode 指针，如果 pos 小于 0，代表倒序位置，如 -1 代表链表尾。如果位置不存在，返回 nil
func (list *List) PosNode(pos int) (*ListNode, bool) {
	if pos < 0 {
		pos += list.Size()
	}

	if pos >= list.Size() || pos < 0 {
		return nil, false
	}

	// 倒序插入
	if list.Size()-pos < pos {

		p := list.BackNode()
		pos = list.Size() - pos - 1
		for i := 0; i < pos; i++ {
			p = p.prev
		}
		return p, true
	}

	p := list.head.next

	for i := 0; i < pos; i++ {
		p = p.next
	}

	return p, true
}

// Range 返回范围之间的链表值，如果 pos 小于 0，代表倒序位置，如 -1 代表链表尾
func (list *List) Range(start, end int) ([]Object, int) {

	if start < 0 {
		start += list.Size()
	}

	// 先处理倒序然后判断 start 和 end 的关系
	if end < 0 {
		if end+list.Size() <= 0 {
			return nil, 0
		} else {
			end += list.Size()
		}
	} else {
		if end > list.Size()-1 {
			end = list.Size() - 1
		}
	}

	if start >= list.Size() || end < 0 {
		return nil, 0
	}
	if start < 0 {
		start = 0
	}

	p := list.head.next

	for i := 0; i < start; i++ {
		p = p.next
	}

	values := make([]Object, end-start+1)

	for i := start; i <= end; i++ {
		values[i-start] = p.Value
		p = p.next
	}
	return values, end - start + 1
}

// Trim 删除[start,end]范围外的元素，start 和 end 可以为负数代表倒序。如果[start,end]为空，则删除所有元素
func (list *List) Trim(start, end int) bool {
	// 先处理倒序然后判断 start 和 end 的关系
	if end < 0 {
		end += list.Size()
	}

	if start < 0 {
		start += list.Size()
	}

	// 空范围直接删除所有元素
	if start > end || (start < 0 && end < 0) || (start >= list.Size() && end >= list.Size()) {
		list.Clear()
		return true
	}

	// 确定范围不为空，进行边界处理
	if start < 0 {
		start = 0
	}
	if end < 0 {
		end = 0
	}
	if start >= list.Size() {
		start = list.Size() - 1
	}
	if end >= list.Size() {
		end = list.Size() - 1
	}

	startNode, ok := list.PosNode(start)
	if !ok {
		return false
	}

	endNode, ok := list.PosNode(end)
	if !ok {
		return false
	}

	list.head.next = startNode
	startNode.prev = list.head

	list.head.prev = endNode
	endNode.next = list.head

	list.size = end - start + 1
	return true
}

// Set 更新指定位置的链表值，如果 pos 小于 0，代表倒序位置，如 -1 代表链表尾
func (list *List) Set(value Object, pos int) bool {
	if pos < 0 {
		pos += list.Size()
	}

	if pos >= list.Size() || pos < 0 {
		return false
	}

	// 倒序插入
	if list.Size()-pos < pos {

		p := list.BackNode()
		pos = list.Size() - pos - 1
		for i := 0; i < pos; i++ {
			p = p.prev
		}
		p.Value = value
		return true
	}

	p := list.head.next

	for i := 0; i < pos; i++ {
		p = p.next
	}
	p.Value = value
	return true
}

// Clear 删除链表的所有节点
func (list *List) Clear() {
	list.head.prev = list.head
	list.head.next = list.head
	list.size = 0
}

func (list *List) Cost() int64 {

	// TODO:
	return -1
}
