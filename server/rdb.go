package server

import (
	"MemTable/logger"
	"github.com/hdt3213/rdb/encoder"
	"io"
	"os"
	"os/exec"
	"path"
	"strconv"
	"sync"
)

const (
	rdbNormal = iota
	rdbWaitForSync
)

type RDBStatus struct {
	rdbLock sync.Mutex // 禁止 rdb 重入锁

	rdbFileStatus int
	rdbWaitNum    int
}

func (s *Server) RDB(file string) bool {

	if !s.rdbLock.TryLock() {
		logger.Warning("RDB: Try Do RDB When Another RDB Process Executing")
		return false
	}

	defer s.rdbLock.Unlock()

	rdbFile, err := os.Create(file + ".tmp")

	if err != nil {
		logger.Error("RDB: Create File Failed", err.Error())
		return false
	}

	defer rdbFile.Close()
	enc := encoder.NewEncoder(rdbFile).EnableCompress()
	err = enc.WriteHeader()
	if err != nil {
		logger.Error("RDB: Write RDB Header Failed", err.Error())
		return false
	}
	auxMap := map[string]string{
		"redis-ver":    "4.0.6",
		"redis-bits":   "64",
		"aof-preamble": "0",
		"repl-id":      s.runID,
		"repl-offset":  strconv.FormatUint(s.offset, 10),
	}
	logger.Info("rdb runid", s.runID)

	for k, v := range auxMap {
		err = enc.WriteAux(k, v)
		if err != nil {
			logger.Error("RDB: Write RDB Aux Failed", err.Error())
			return false
		}
	}

	for index, db := range s.dbs {

		if db.Size() == 0 {
			continue
		}

		err = enc.WriteDBHeader(uint(index), uint64(db.Size()), uint64(db.TTLSize()))
		if err != nil {
			logger.Error("RDB: Write RDB DB Header Failed", err.Error())
			return false
		}
		err = db.Encode(enc)
		if err != nil {
			logger.Error("RDB: Write RDB DB Content Failed", err.Error())
			return false
		}
	}

	err = enc.WriteEnd()
	if err != nil {
		logger.Error("RDB: Write RDB End Failed", err.Error())
		return false
	}

	_ = os.Rename(file+".tmp", file)

	return true
}

func (s *Server) BGRDB() bool {

	// 复制 aof
	file1, err := os.Open(path.Join(s.dir, s.aofFile))
	if err != nil {
		logger.Error("AOF Rewrite: Can't Load AOF File")
		return false
	}
	file2, err := os.OpenFile(path.Join(s.dir, s.aofFile+".tmp"), os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		logger.Error("AOF Rewrite: Can't Create Temporary AOF File")
		return false
	}

	defer file1.Close()
	defer file2.Close()

	_, err = io.Copy(file2, file1)
	if err != nil {
		logger.Error("AOF Rewrite: AOF Copy Error")
		return false
	}

	// aof 启动一个 server
	ws := NewServer("")
	ws.runID = s.runID
	ws.offset = s.offset
	ws.rdbOffset = s.rdbOffset

	go func() {

		ws.recoverFromAOF(path.Join(s.dir, s.aofFile+".tmp"))
		// server 进行恢复后，保存 rdb
		ok := ws.RDB(path.Join(s.dir, s.rdbFile))

		logger.Info("BGSave Finished")

		if !ok {
			logger.Error("BGSave Failed")
		}

		// 清理文件
		_ = os.Remove(s.aofFile + ".tmp")

	}()
	return true
}

func (s *Server) waitForRDBFinished() {
	s.rdbLock.Lock()
	defer s.rdbLock.Unlock()

}

func (s *Server) recoverFromRDB(aofFile, rdbFile string) {

	arg := []string{"-c", "protocol", "-f", aofFile, rdbFile}
	cmd := exec.Command("rdb", arg...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		logger.Error("Read RDB:", err.Error())
		return
	}

	if cmd.ProcessState.ExitCode() != 0 {
		logger.Error("Load RDB:", string(output))
		return
	}

	s.recoverFromAOF(aofFile)

	if !s.aofEnabled {
		// 删除 aof 文件
		_ = os.Remove(aofFile)
	}
}
