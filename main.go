package main

import (
	"MemTable/config"
	"MemTable/server"
	"fmt"
	"path"
	"strings"
)

func main() {
	p, f := path.Split("/hjg/fggf/gf")
	segs := strings.Split(p, "/")
	println(p)
	println(f)
	for _, seg := range segs {
		println(seg)
	}

	return

	s := server.NewServer(fmt.Sprintf("%s:%d", config.Conf.Host, config.Conf.Port))
	s.Start()

}
