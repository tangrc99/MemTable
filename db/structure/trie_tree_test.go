package structure

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTrieTree(t *testing.T) {

	tree := NewTrieTree()

	path0 := []string{"a"}
	path1 := []string{"a", "b", "c"}
	path2 := []string{"a", "b", "e"}
	path3 := []string{"a", "b", "c", "d"}
	path4 := []string{"a", "b", "e", "f"}
	tree.AddNode(path0, Slice("p0"))
	tree.AddNode(path1, Slice("p1"))
	tree.AddNode(path2, Slice("p2"))
	tree.AddNode(path3, Slice("p3"))
	tree.AddNode(path4, Slice("p4"))

	if nodes := tree.AllLeafNodeInPath(path0); len(nodes) != 1 {
		t.Error("AllLeafNodeInPath Failed")
	}

	if nodes := tree.AllLeafNodeInPathRecursive(path0); len(nodes) != 5 {
		t.Error("AllLeafNodeInPathRecursive Failed")
	}

	if !tree.IsPathExist(path1) || !tree.IsPathExist(path2) || !tree.IsPathExist(path3) || !tree.IsPathExist(path4) {
		t.Error("Path Failed")
	}

	path5 := []string{"a", "b"}

	if tree.IsPathExist(path5) {
		t.Error("Path Failed")
	}

	if !tree.DeletePath(path4) || !tree.IsPathExist(path2) || tree.IsPathExist(path4) {
		t.Error("DeletePath Failed")
	}

	_, ok := tree.AddNodeIfNotLeaf(path2, Slice("p2"))
	assert.False(t, ok)

	_, ok = tree.GetLeafNode(path4)
	assert.False(t, ok)

	_, ok = tree.GetValue(path4)
	assert.False(t, ok)

	_, ok = tree.AddNodeIfNotLeaf(path4, Slice("p2"))
	assert.True(t, ok)

	_, ok = tree.GetLeafNode(path4)
	assert.True(t, ok)

	_, ok = tree.GetValue(path4)
	assert.True(t, ok)
}
