package sys_status

import (
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/process"
	"os"
	"time"
)

type SysStatus struct {
	// 系统内存状况
	Total   uint64 // 本机内存
	MemUsed uint64
	MemFree uint64

	// 进程内存状况
	RSS uint64 // 物理内存
	VMS uint64 // 虚拟内存

	// CPU
	CPUPercents float64
}

func (s *SysStatus) UpdateSysStatus() {
	CPUPercents, _ := cpu.Percent(time.Millisecond, true)
	sum := float64(0)
	for _, n := range CPUPercents {
		sum += n
	}
	s.CPUPercents = sum / float64(len(CPUPercents))

	memInfo, _ := mem.VirtualMemory()
	s.MemUsed = memInfo.Used
	s.MemFree = memInfo.Free
	s.Total = memInfo.Total

	newProcess, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return
	}

	pMemInfo, _ := newProcess.MemoryInfo()
	s.RSS = pMemInfo.RSS
	s.VMS = pMemInfo.VMS

}
