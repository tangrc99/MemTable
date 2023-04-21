package readline

import (
	"github.com/tangrc99/MemTable/db/structure"
	"strings"
)

type Hint struct {
	name   string
	helper string
}

func NewHint(name, helper string) *Hint {
	return &Hint{
		name:   name,
		helper: helper,
	}
}

func (h *Hint) Cost() int64 {
	return int64(len(h.name) + len(h.helper))
}

// Completer 是基于前缀树的单词补足结构体
type Completer struct {
	trieTree *structure.TrieTree
}

func NewCompleter() *Completer {
	return &Completer{
		trieTree: structure.NewTrieTree(),
	}
}

// Register 将单词注册到 Completer 中
func (c *Completer) Register(hint *Hint) {
	if hint.name == "" {
		return
	}
	path := strings.Split(hint.name, "")
	c.trieTree.AddNode(path, hint)
}

// Query 查询以当前单词为前缀的单词，返回这些单词的切片
func (c *Completer) Query(word string) []string {

	path := strings.Split(word, "")
	nodes := c.trieTree.AllLeafNodeInPathRecursive(path)

	matched := make([]string, 0, len(nodes))
	// 类型转换
	for _, node := range nodes {
		matched = append(matched, node.Value.(*Hint).name)
	}
	return matched
}

// Exist 查询当前单词是否存在
func (c *Completer) Exist(word string) bool {
	path := strings.Split(word, "")
	return c.trieTree.IsPathExist(path)
}

// GetHelper 查询当前命令是否存在帮助
func (c *Completer) GetHelper(word string) (string, bool) {
	path := strings.Split(word, "")
	v, exist := c.trieTree.GetValue(path)
	if !exist {
		return "", false
	}
	return v.(*Hint).helper, true
}
