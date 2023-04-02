package server

import (
	"github.com/tangrc99/MemTable/logger"
	"time"
)

// runInNewGoroutine 从协程池中获取或直接创建一个协程来运行指定任务
func (s *Server) runInNewGoroutine(task func()) bool {

	if allowed := s.checkAndEvictClientsIfNeeded(); !allowed {
		logger.Infof("Too many clients clis %d max clis %d", s.clis.Size(), s.maxClients)
		return false
	}

	if s.gopool != nil {
		s.gopool.Schedule(task)
	} else {
		go task()
	}

	return true
}

// checkAndEvictClientsIfNeeded 检查当前是否允许新建客户端，如果不允许则根据一定策略淘汰。
func (s *Server) checkAndEvictClientsIfNeeded() (allowed bool) {

	// 尝试删除
	sz := s.clis.Size()

	// 如果超出客户端上限，尝试淘汰旧连接
	if s.maxClients > 0 && sz > s.maxClients {
		toRemove := sz - s.maxClients + 1
		s.clis.RemoveLongNotUsed(sz-s.maxClients+1, 2*toRemove, time.Second)
		// 成功删除
		return s.clis.Size() < s.maxClients
	}

	// 如果超出协程上限，尝试淘汰一个客户端
	if sz >= 2*s.gopool.Maximum() {
		s.clis.RemoveLongNotUsed(1, 5, time.Second)
		return 2*s.gopool.Maximum()-s.clis.Size() >= 1
	}

	return true
}
