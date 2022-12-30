package structure

type TrieTreeNode struct {
	Value    string                   //值
	isLeaf   bool                     // 判别是否为叶子节点
	parent   *TrieTreeNode            // 父结点
	children map[string]*TrieTreeNode // 子节点链表
	tree     *TrieTree                // 所属的树
}

func newTrieTreeNode(v string, leaf bool, owner *TrieTree) *TrieTreeNode {
	return &TrieTreeNode{
		Value:    v,
		isLeaf:   leaf,
		children: make(map[string]*TrieTreeNode),
		tree:     owner,
	}
}

type TrieTree struct {
	root  *TrieTreeNode // 根节点
	count int           // 节点数量
}

func NewIndexTree() *TrieTree {
	tree := TrieTree{
		root: newTrieTreeNode("", false, nil),
	}
	tree.root.tree = &tree
	return &tree
}

func (tree *TrieTree) AddNode(path []string, value any) {

}

func (tree *TrieTree) DeleteNode() {

}

func (tree *TrieTree) IsLeafNode() {

}

func (tree *TrieTree) AllNodeInPath() {

}
