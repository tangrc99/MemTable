package server

import (
	"github.com/tangrc99/MemTable/utils/sys_status"
)

type Status struct {
	sys_status.SysStatus
}

func NewStatus() *Status {

	s := &Status{}

	s.UpdateSysStatus()

	return s
}

func (s *Status) UpdateStatus() {
	s.UpdateSysStatus()
}
