package structure

type ListNode struct {
	next, prev *ListNode
	list       *List
	Value      any
}

func (node *ListNode) Next() *ListNode {
	if node.next == node.list.head {
		return nil
	}
	return node.next
}

func (node *ListNode) Prev() *ListNode {
	if node.prev == node.list.head {
		return nil
	}
	return node.prev
}

type List struct {
	head *ListNode
	size int
}

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

func (list *List) FrontNode() *ListNode {
	if list.head.next == list.head {
		return nil
	}
	return list.head.next
}

func (list *List) BackNode() *ListNode {
	if list.head.prev == list.head {
		return nil
	}
	return list.head.prev
}

// Front 如果不存在值会返回 nil
func (list *List) Front() any {
	return list.head.next.Value
}

// Back 如果不存在值会返回 nil
func (list *List) Back() any {
	return list.head.prev.Value
}

func (list *List) InsertAfterNode(value any, at *ListNode) *ListNode {
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

func (list *List) InsertBeforeNode(value any, at *ListNode) *ListNode {
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

func (list *List) RemoveNode(at *ListNode) any {

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

func (list *List) PushFront(value any) {
	list.InsertAfterNode(value, list.head)
}

func (list *List) PushBack(value any) {
	list.InsertBeforeNode(value, list.head)
}

func (list *List) PopFront() any {
	if list.Size() == 0 {
		return nil
	}

	return list.RemoveNode(list.head.next)
}

func (list *List) PopBack() any {
	if list.Size() == 0 {
		return nil
	}
	return list.RemoveNode(list.head.prev)
}

func (list *List) Size() int {
	return list.size
}

func (list *List) Empty() bool {
	return list.size == 0
}

// InsertAfter 将元素插入到给定位置后面,pos > 0
func (list *List) InsertAfter(value any, pos int) bool {
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

// InsertBefore 将元素插入到给定位置后面,pos > 0
func (list *List) InsertBefore(value any, pos int) bool {

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

// Pos 大于 0 正向遍历，小于 0 反向遍历
func (list *List) Pos(pos int) (any, bool) {
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

func (list *List) Range(start, end int) ([]any, int) {

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

	if start >= list.Size() || start < 0 {
		return nil, 0
	}

	p := list.head.next

	for i := 0; i < start; i++ {
		p = p.next
	}

	values := make([]any, end-start+1)

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

func (list *List) RemoveValue(value any, nums int) int {
	deleted := 0

	for cur := list.FrontNode(); cur != list.BackNode(); {

		if cur.Value == value {
			node := cur
			cur = cur.next
			list.RemoveNode(node)
			if deleted++; deleted >= nums {
				break
			}
		} else {
			cur = cur.next
		}

	}
	return deleted
}

// Set 大于 0 正向遍历，小于 0 反向遍历
func (list *List) Set(value any, pos int) bool {
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

func (list *List) Clear() {
	list.head.prev = list.head
	list.head.next = list.head
	list.size = 0
}
