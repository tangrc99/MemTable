package main

import (
	"MemTable/config"
	"MemTable/server"
	"fmt"
)

func main() {

	s := server.NewServer(fmt.Sprintf("%s:%d", config.Conf.Host, config.Conf.Port))
	s.Start()

	return
}
