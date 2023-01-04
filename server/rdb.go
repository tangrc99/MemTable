package server

import (
	"MemTable/logger"
	"github.com/hdt3213/rdb/encoder"
	"io"
	"os"
	"sync"
)

// 禁止 RDB 重入锁
var lock sync.Mutex

const (
	rdbNormal = iota
	rdbWaitForSync
)

var rdbWaitNum int = 0

var rdbFileStatus int

func (s *Server) RDB(file string) {

	//fixme : logs and error handler

	rdbFile, err := os.Create(file)
	if err != nil {
		panic(err)
	}
	defer rdbFile.Close()
	enc := encoder.NewEncoder(rdbFile).EnableCompress()
	err = enc.WriteHeader()
	if err != nil {
		panic(err)
	}
	auxMap := map[string]string{
		"redis-ver":    "4.0.6",
		"redis-bits":   "64",
		"aof-preamble": "0",
	}
	for k, v := range auxMap {
		err = enc.WriteAux(k, v)
		if err != nil {
			panic(err)
		}
	}

	for index, db := range s.dbs {

		if db.Size() == 0 {
			continue
		}

		err = enc.WriteDBHeader(uint(index), uint64(db.Size()), uint64(db.TTLSize()))
		if err != nil {
			panic(err)
		}
		err = db.Encode(enc)
		if err != nil {
			panic(err)
		}
	}

	if err != nil {
		panic(err)
	}
	err = enc.WriteEnd()
	if err != nil {
		panic(err)
	}
}

func (s *Server) BGRDB() bool {

	if !lock.TryLock() {
		return false
	}

	// 复制 aof
	file1, err := os.Open(s.aof.filename)
	if err != nil {
		lock.Unlock()
		logger.Error("AOF Rewrite: Can't Load AOF File")
		return false
	}
	file2, err := os.OpenFile(s.aof.filename+".tmp", os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		lock.Unlock()
		logger.Error("AOF Rewrite: Can't Create Temporary AOF File")
		return false
	}

	defer file1.Close()
	defer file2.Close()

	_, err = io.Copy(file2, file1)
	if err != nil {
		lock.Unlock()
		logger.Error("AOF Rewrite: AOF Copy Error")
		return false
	}

	go func() {
		// aof 启动一个 server
		ws := NewServer("")
		ws.recoverFromAOF(s.aof.filename + ".tmp")
		// server 进行恢复后，保存 rdb
		ws.RDB("dump1.rdb")
		// 清理文件
		_ = os.Remove(s.aof.filename + ".tmp")
		_ = os.Rename("dump1.rdb", "dump.rdb")
		lock.Unlock()
	}()
	return true
}

func (s *Server) waitForRDBFinished() {
	lock.Lock()
	defer lock.Unlock()
}

func (s *Server) recoverFromRDB(file string) {

}
