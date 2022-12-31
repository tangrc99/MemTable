package main

import (
	"MemTable/config"
	"MemTable/db/structure"
	"MemTable/server"
	"fmt"
)

func TrieTreeTest(s *[]string) {
	//tree := structure.NewTrieTree()
	//
	//path1 := []string{"a", "b", "c"}
	//path2 := []string{"a", "b", "e"}
	//
	//tree.AddNode(path1, "d", "path1")
	//tree.AddNode(path2, "f", "path2")
	//path3 := []string{"a", "b", "c", "d"}
	//path4 := []string{"a", "b", "e", "f"}
	//
	//path0 := []string{"b"}
	//
	//nodes := tree.AllLeafNodeInPath(path0)
	//for _, node := range nodes {
	//	println(node.Value.(string))
	//}
	//
	//println(tree.IsPathExist(path1))
	//println(tree.IsPathExist(path3))
	//println(tree.IsPathExist(path4))
	//
	//println(tree.DeletePath(path3))
	//println(tree.IsPathExist(path3))
	//println(tree.DeletePath(path4))
	//println(tree.IsPathExist(path4))
}

func main() {

	sl := structure.NewSkipList(3)
	sl.Insert(1, "1")
	sl.Insert(2, "2")
	sl.Insert(4, "4")
	sl.Insert(5, "5")
	vs, _ := sl.Pos(-2, -1)
	for _, v := range vs {
		println(v.(string))
	}
	//println(sl.Size())
	//println(sl.Delete(2))
	//str := "/a"
	//
	//paths := strings.Split(str, "/")
	//
	//for i, path := range paths {
	//	println(i)
	//	println(path)
	//}
	//return

	s := server.NewServer(fmt.Sprintf("%s:%d", config.Conf.Host, config.Conf.Port))
	s.Start()

}
