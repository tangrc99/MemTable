package structure

import (
	"github.com/tidwall/btree"
	"unsafe"
)

const trieTreeNodeBasicCost = int64(unsafe.Sizeof(trieTreeNode{}))

type trieTreeNode struct {
	Key      string                            // 键
	Value    Object                            // 值
	isLeaf   bool                              // 判别是否为叶子节点
	parent   *trieTreeNode                     // 父结点
	children *btree.Map[string, *trieTreeNode] // 子节点链表
	tree     *TrieTree                         // 所属的树
}

func newTrieTreeNode(key string, value Object, leaf bool, parent *trieTreeNode, owner *TrieTree) *trieTreeNode {
	return &trieTreeNode{
		Key:      key,
		Value:    value,
		isLeaf:   leaf,
		parent:   parent,
		children: btree.NewMap[string, *trieTreeNode](2),
		tree:     owner,
	}
}

func (n *trieTreeNode) Cost() int64 {
	return n.Value.Cost() + trieTreeNodeBasicCost
}

const trieTreeBasicCost = int64(unsafe.Sizeof(TrieTree{}))

// TrieTree 是一个前缀树容器
type TrieTree struct {
	root  *trieTreeNode // 根节点
	count int           // 叶节点数量
	cost  int64
}

// NewTrieTree 创建一个前缀树 TrieTree 并返回指针
func NewTrieTree() *TrieTree {
	tree := TrieTree{
		root: newTrieTreeNode("", nil, false, nil, nil),
		cost: trieTreeBasicCost,
	}
	tree.root.tree = &tree
	return &tree
}

// AddNode 将值插入到指定路径，该操作可能会覆盖旧值
func (tree *TrieTree) AddNode(paths []string, value Object) *trieTreeNode {
	cur := tree.root

	for _, path := range paths {
		node, exists := cur.children.Get(path)
		if !exists {
			node = newTrieTreeNode(path, Nil{}, false, cur, tree)
			tree.cost += node.Cost()
			cur.children.Set(path, node)
		}
		cur = node
	}

	if !cur.isLeaf {
		tree.count++
	}
	cur.isLeaf = true
	tree.cost -= cur.Cost()
	cur.Value = value
	tree.cost += cur.Cost()

	return cur
}

// AddNodeIfNotLeaf 将值插入到指定路径，若路径已经为叶子节点，该操作不会覆盖旧值，返回 nil,false。
func (tree *TrieTree) AddNodeIfNotLeaf(paths []string, value Object) (*trieTreeNode, bool) {

	cur := tree.root

	for _, path := range paths {
		node, exists := cur.children.Get(path)
		if !exists {
			node = newTrieTreeNode(path, Nil{}, false, cur, tree)
			tree.cost += node.Cost()
			cur.children.Set(path, node)
		}
		cur = node
	}

	if cur.isLeaf {
		return cur, false
	}

	cur.isLeaf = true
	cur.Value = value
	tree.count++
	tree.cost += cur.Cost()

	return cur, true
}

// DeleteLeafNode 删除前缀树节点，如果节点不是叶节点，返回 false
func (tree *TrieTree) DeleteLeafNode(node *trieTreeNode) bool {
	if node == nil || !node.isLeaf || node.tree != tree {
		return false
	}

	cur := node.parent
	cur.children.Delete(node.Key)
	tree.cost -= node.Cost()

	for cur != nil && !cur.isLeaf && cur.children.Len() == 0 {
		nxt := cur.parent
		if nxt != nil {
			nxt.children.Delete(cur.Key)
			tree.cost -= cur.Cost()
		}
		cur = nxt
	}
	tree.count--
	return true
}

// DeletePath 删除前缀树中的路径，如果路径不是叶节点，返回 false
func (tree *TrieTree) DeletePath(paths []string) bool {
	cur := tree.root

	// 遍历到最底下的节点
	for _, path := range paths {
		node, exists := cur.children.Get(path)
		if !exists {
			return false
		}
		cur = node
	}

	return tree.DeleteLeafNode(cur)
}

// IsPathExist 判断给定路径是否为前缀树中的叶节点
func (tree *TrieTree) IsPathExist(paths []string) bool {

	cur := tree.root

	for _, path := range paths {
		node, exists := cur.children.Get(path)
		if !exists {
			return false
		}
		cur = node
	}
	return cur.isLeaf
}

// GetValue 获取给定路径的值，若路径不存在或不为叶节点，返回 nil,false
func (tree *TrieTree) GetValue(paths []string) (Object, bool) {
	cur := tree.root

	for _, path := range paths {
		node, exists := cur.children.Get(path)
		if !exists {
			return nil, false
		}
		cur = node
	}
	return cur.Value, cur.isLeaf
}

// GetLeafNode 获取路径对应的叶节点指针，若路径不存在或不为叶节点，返回 nil,false
func (tree *TrieTree) GetLeafNode(paths []string) (*trieTreeNode, bool) {
	cur := tree.root

	for _, path := range paths {
		node, exists := cur.children.Get(path)
		if !exists {
			return nil, false
		}
		cur = node
	}

	if cur.isLeaf {
		return cur, true
	}

	return nil, false
}

// AllLeafNodeInPath 返回当前路径以及路径下的叶子节点（非递归）
func (tree *TrieTree) AllLeafNodeInPath(paths []string) []*trieTreeNode {
	cur := tree.root

	for _, path := range paths {
		node, exists := cur.children.Get(path)
		if !exists {
			return nil
		}
		cur = node
	}
	r := make([]*trieTreeNode, 0)

	// 判断当前节点
	if cur.isLeaf {
		r = append(r, cur)
	}
	// 判断子节点
	for it := cur.children.Iter(); it.Next(); {
		if it.Value().isLeaf {
			r = append(r, it.Value())
		}
	}

	return r
}

// dfsGetLeafNodes 返回当前路径以及路径下的叶子节点（递归）
func (tree *TrieTree) dfsGetLeafNodes(node *trieTreeNode, r *[]*trieTreeNode) {

	if node == nil || node.tree != tree {
		return
	}

	if node.isLeaf {
		*r = append(*r, node)
	}

	for it := node.children.Iter(); it.Next(); {
		tree.dfsGetLeafNodes(it.Value(), r)
	}
}

// bfsGetLeafNodes 返回当前路径以及路径下的叶子节点（递归）
func (tree *TrieTree) bfsGetLeafNodes(node *trieTreeNode) []*trieTreeNode {

	var nodes []*trieTreeNode
	q := NewList()
	q.PushFront(node)
	for !q.Empty() {
		n := q.PopFront().(*trieTreeNode)
		for it := n.children.Iter(); it.Next(); {
			q.PushBack(it.Value())
		}
		if n.isLeaf {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

type SearchOrder = int

const (
	StandardOrder SearchOrder = iota
	DictionaryOrder
)

// AllLeafNodeInPathRecursive 返回当前路径以及路径下的叶子节点（递归）
func (tree *TrieTree) AllLeafNodeInPathRecursive(paths []string, order SearchOrder) []*trieTreeNode {
	cur := tree.root

	// 找到起始路径
	for _, path := range paths {
		node, exists := cur.children.Get(path)
		if !exists {
			return nil
		}
		cur = node
	}

	// 标准序，使用 bfs 搜索
	if order == StandardOrder {
		return tree.bfsGetLeafNodes(cur)
	}

	// 字典序，使用 dfs 搜索
	r := make([]*trieTreeNode, 0)
	tree.dfsGetLeafNodes(cur, &r)
	return r
}

func (tree *TrieTree) Cost() int64 {
	return tree.cost
}
