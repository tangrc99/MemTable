package structure

type TrieTreeNode struct {
	Key      string                   // 键
	Value    any                      // 值
	isLeaf   bool                     // 判别是否为叶子节点
	parent   *TrieTreeNode            // 父结点
	children map[string]*TrieTreeNode // 子节点链表
	tree     *TrieTree                // 所属的树
}

func newTrieTreeNode(key string, value any, leaf bool, parent *TrieTreeNode, owner *TrieTree) *TrieTreeNode {
	return &TrieTreeNode{
		Key:      key,
		Value:    value,
		isLeaf:   leaf,
		parent:   parent,
		children: make(map[string]*TrieTreeNode),
		tree:     owner,
	}
}

type TrieTree struct {
	root  *TrieTreeNode // 根节点
	count int           // 叶节点数量
}

func NewTrieTree() *TrieTree {
	tree := TrieTree{
		root: newTrieTreeNode("", nil, false, nil, nil),
	}
	tree.root.tree = &tree
	return &tree
}

func (tree *TrieTree) AddNode(paths []string, value any) *TrieTreeNode {
	cur := tree.root

	for _, path := range paths {
		node, exists := cur.children[path]
		if !exists {
			node = newTrieTreeNode(path, nil, false, cur, tree)
			cur.children[path] = node
		}
		cur = node
	}

	cur.isLeaf = true
	cur.Value = value
	tree.count++
	return cur
}

func (tree *TrieTree) AddNodeIfNotLeaf(paths []string, value any) (*TrieTreeNode, bool) {

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

func (tree *TrieTree) DeleteLeafNode(node *TrieTreeNode) bool {
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

func (tree *TrieTree) GetLeafNode(paths []string) (*TrieTreeNode, bool) {
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
func (tree *TrieTree) AllLeafNodeInPath(paths []string) []*TrieTreeNode {
	cur := tree.root

	for _, path := range paths {
		node, exists := cur.children[path]
		if !exists {
			return nil
		}
		cur = node
	}
	r := make([]*TrieTreeNode, 0)

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
func (tree *TrieTree) dfsGetLeafNodes(node *TrieTreeNode, r *[]*TrieTreeNode) {

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
func (tree *TrieTree) AllLeafNodeInPathRecursive(paths []string) []*TrieTreeNode {
	cur := tree.root

	for _, path := range paths {
		node, exists := cur.children[path]
		if !exists {
			return nil
		}
		cur = node
	}
	r := make([]*TrieTreeNode, 0)

	tree.dfsGetLeafNodes(cur, &r)

	return r
}
