package structure

type trieTreeNode struct {
	Key      string                   // 键
	Value    any                      // 值
	isLeaf   bool                     // 判别是否为叶子节点
	parent   *trieTreeNode            // 父结点
	children map[string]*trieTreeNode // 子节点链表
	tree     *TrieTree                // 所属的树
}

func newTrieTreeNode(key string, value any, leaf bool, parent *trieTreeNode, owner *TrieTree) *trieTreeNode {
	return &trieTreeNode{
		Key:      key,
		Value:    value,
		isLeaf:   leaf,
		parent:   parent,
		children: make(map[string]*trieTreeNode),
		tree:     owner,
	}
}

// TrieTree 是一个前缀树容器
type TrieTree struct {
	root  *trieTreeNode // 根节点
	count int           // 叶节点数量
}

// NewTrieTree 创建一个前缀树 TrieTree 并返回指针
func NewTrieTree() *TrieTree {
	tree := TrieTree{
		root: newTrieTreeNode("", nil, false, nil, nil),
	}
	tree.root.tree = &tree
	return &tree
}

// AddNode 将值插入到指定路径，该操作可能会覆盖旧值
func (tree *TrieTree) AddNode(paths []string, value any) *trieTreeNode {
	cur := tree.root

	for _, path := range paths {
		node, exists := cur.children[path]
		if !exists {
			node = newTrieTreeNode(path, nil, false, cur, tree)
			cur.children[path] = node
		}
		cur = node
	}

	if !cur.isLeaf {
		tree.count++
	}
	cur.isLeaf = true
	cur.Value = value
	return cur
}

// AddNodeIfNotLeaf 将值插入到指定路径，若路径已经为叶子节点，该操作不会覆盖旧值，返回 nil,false。
func (tree *TrieTree) AddNodeIfNotLeaf(paths []string, value any) (*trieTreeNode, bool) {

	cur := tree.root

	for _, path := range paths {
		node, exists := cur.children[path]
		if !exists {
			node = newTrieTreeNode(path, nil, false, cur, tree)
			cur.children[path] = node
		}
		cur = node
	}

	if cur.isLeaf {
		return cur, false
	}

	cur.isLeaf = true
	cur.Value = value
	tree.count++
	return cur, true
}

// DeleteLeafNode 删除前缀树节点，如果节点不是叶节点，返回 false
func (tree *TrieTree) DeleteLeafNode(node *trieTreeNode) bool {
	if node == nil || !node.isLeaf || node.tree != tree {
		return false
	}

	cur := node.parent
	delete(cur.children, node.Key)

	for cur != nil && !cur.isLeaf && len(cur.children) == 0 {
		nxt := cur.parent
		if nxt != nil {
			delete(nxt.children, cur.Key)
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
		node, exists := cur.children[path]
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
		node, exists := cur.children[path]
		if !exists {
			return false
		}
		cur = node
	}
	return cur.isLeaf
}

// GetValue 获取给定路径的值，若路径不存在或不为叶节点，返回 nil,false
func (tree *TrieTree) GetValue(paths []string) (any, bool) {
	cur := tree.root

	for _, path := range paths {
		node, exists := cur.children[path]
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
		node, exists := cur.children[path]
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
		node, exists := cur.children[path]
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
	for _, node := range cur.children {
		if node.isLeaf {
			r = append(r, node)
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

	for _, child := range node.children {

		tree.dfsGetLeafNodes(child, r)
	}
}

// AllLeafNodeInPathRecursive 返回当前路径以及路径下的叶子节点（递归）
func (tree *TrieTree) AllLeafNodeInPathRecursive(paths []string) []*trieTreeNode {
	cur := tree.root

	for _, path := range paths {
		node, exists := cur.children[path]
		if !exists {
			return nil
		}
		cur = node
	}
	r := make([]*trieTreeNode, 0)

	tree.dfsGetLeafNodes(cur, &r)

	return r
}
