package sys_status

import (
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	"time"
)

type SysStatus struct {
	Now time.Time // 服务端全局时间

	// 内存状况
	Total   uint64 // 本机内存
	MemUsed uint64
	MemFree uint64

	// CPU
	CPUPercents []float64
}

func NewSysStatus() *SysStatus {

	s := &SysStatus{}
	s.UpdateSysStatus()
	return s
}

func (s *SysStatus) UpdateSysStatus() {
	s.CPUPercents, _ = cpu.Percent(time.Millisecond, true)

	memInfo, _ := mem.VirtualMemory()
	s.MemUsed = memInfo.Used
	s.MemFree = memInfo.Free
	s.Total = memInfo.Total

	s.Now = time.Now()

}
