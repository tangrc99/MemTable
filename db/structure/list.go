package structure

type listNode struct {
	next, prev *listNode
	list       *List
	Value      any
}

type List struct {
	head *listNode
	size int
}

func NewList() *List {
	l := List{
		head: nil,
		size: 0,
	}
	head := &listNode{
		next: nil, prev: nil, list: &l, Value: nil,
	}
	head.prev = head
	head.next = head
	l.head = head
	return &l
}

func (list *List) front() *listNode {
	return list.head.next
}
func (list *List) back() *listNode {
	return list.head.prev
}
func (list *List) Front() any {
	return list.head.next.Value
}

func (list *List) Back() any {
	return list.head.prev.Value
}

func (list *List) insertAfter(value any, at *listNode) *listNode {

	if at.list != list {
		return nil
	}

	next := at.next
	node := listNode{
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

func (list *List) insertBefore(value any, at *listNode) *listNode {

	if at.list != list {
		return nil
	}

	prev := at.prev
	node := listNode{
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

func (list *List) remove(at *listNode) any {

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
	list.insertAfter(value, list.head)
}

func (list *List) PushBack(value any) {
	list.insertBefore(value, list.head)
}

func (list *List) PopFront() any {
	return list.remove(list.head.next)
}

func (list *List) PopBack() any {
	return list.remove(list.head.prev)
}

func (list *List) Size() int {
	return list.size
}

func (list *List) Empty() bool {
	return list.size == 0
}

// InsertAfter 将元素插入到给定位置后面
func (list *List) InsertAfter(value any, pos int) bool {

	if pos >= list.Size() || pos < 0 {
		return false
	}

	// 倒序插入
	if list.Size()-pos < pos {

		p := list.back()
		pos = list.Size() - pos - 1
		for i := 0; i < pos; i++ {
			p = p.prev
		}
		list.insertAfter(value, p)
		return true
	}

	p := list.head.next

	for i := 0; i < pos; i++ {
		p = p.next
	}

	return nil != list.insertAfter(value, p)
}

// InsertBefore 将元素插入到给定位置后面
func (list *List) InsertBefore(value any, pos int) bool {

	if pos >= list.Size() || pos < 0 {
		return false
	}

	// 倒序插入
	if list.Size()-pos < pos {

		p := list.back()
		pos = list.Size() - pos - 1
		for i := 0; i < pos; i++ {
			p = p.prev
		}
		list.insertBefore(value, p)
		return true
	}

	p := list.head.next

	for i := 0; i < pos; i++ {
		p = p.next
	}

	return nil != list.insertBefore(value, p)

}

func (list *List) Range(start, end int) ([]any, bool) {
	if start >= list.Size() || start > end {
		return nil, false
	}
	if end > list.Size()-1 || end < 0 {
		end = list.Size() - 1
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
	return values, true
}

func (list *List) RemoveValue(value any, nums int) int {
	deleted := 0

	for cur := list.front(); cur != list.back(); {

		if cur.Value == value {
			node := cur
			cur = cur.next
			list.remove(node)
			if deleted++; deleted >= nums {
				break
			}
		} else {
			cur = cur.next
		}

	}
	return deleted
}

func (list *List) Set(value any, pos int) bool {
	if pos >= list.Size() {
		return false
	}
	p := list.front()

	for i := 0; i < pos; i++ {
		p = p.next
	}
	p.Value = value
	return true
}
