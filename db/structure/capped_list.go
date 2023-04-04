package structure

import "unsafe"

const cappedListNodeBasicCost = int64(unsafe.Sizeof(CappedListNode{}))

// CappedListNode 是一个单向链表节点
type CappedListNode struct {
	next  *CappedListNode
	list  *CappedList
	Value Object
}

// nextNode 获取该节点的下一个节点，该函数会跳过哨兵节点
func (node *CappedListNode) nextNode() *CappedListNode {
	if node.next == node.list.head {
		return node.next.next
	}
	return node.next
}

func (node *CappedListNode) Cost() int64 {
	return cappedListNodeBasicCost + node.Value.Cost()
}

const cappedListBasicCost = int64(unsafe.Sizeof(CappedList{}))

// CappedList 是一个以链表形式组织的循环列表
type CappedList struct {
	head *CappedListNode // 哨兵位置
	cur  *CappedListNode // 当前写入的位置
	size int             // 节点数量
	max  int             // 最大节点数量
	cost int64           // 占用的内存
}

func NewCappedList(max int) *CappedList {
	l := &CappedList{
		size: 0,
		max:  max,
		cost: cappedListBasicCost,
	}
	l.head = &CappedListNode{
		list:  l,
		next:  nil,
		Value: nil,
	}
	l.head.next = l.head
	l.cur = l.head
	return l
}

// Append 会在链表尾追加一个节点，如果节点达到上限会自动覆盖链表头
func (cl *CappedList) Append(value Object) {

	if cl.size < cl.max {
		node := &CappedListNode{
			next:  cl.head,
			Value: value,
			list:  cl,
		}
		cl.cur.next = node
		cl.cur = node
		cl.size++
		cl.cost += node.Cost()
		return
	}

	cl.cur = cl.cur.nextNode()
	oldCost := cl.cur.Cost()
	cl.cur.Value = value
	newCost := cl.cur.Cost()
	cl.cost += newCost - oldCost
}

// GetN 从链表头开始，获取 N 个节点的值
func (cl *CappedList) GetN(num int) []Object {

	if num > cl.size {
		num = cl.size
	}

	ret := make([]Object, 0, num)

	for cur := cl.cur.nextNode(); num > 0; num-- {
		ret = append(ret, cur.Value)
		cur = cur.nextNode()
	}
	return ret
}

// Clear 清除链表
func (cl *CappedList) Clear() {
	cl.size = 0
	cl.head.next = cl.head
	cl.cur = cl.head
	cl.cost = cappedListBasicCost
}

func (cl *CappedList) Cost() int64 {
	return cl.cost
}

func (cl *CappedList) Size() int {
	return cl.size
}
