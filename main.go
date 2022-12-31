package main

import (
	"MemTable/config"
	"MemTable/db"
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

	chs := db.NewChannels()
	notify := make(chan []byte)
	chs.SubscribePath("/a/b", "1", &notify)

	go func() {
		println(chs.Publish("/a", []byte("sss")))
	}()

	println(<-notify)

	chs.UnSubscribePath("/a/b", "1")
	go func() {
		println(chs.Publish("/a", []byte("sss")))
	}()

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
