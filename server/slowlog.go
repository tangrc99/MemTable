package server

import (
	"github.com/tangrc99/MemTable/db/structure"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/global"
	"unsafe"
)

const slowLogEntryBasicCost = int64(unsafe.Sizeof(slowLogEntry{}))

// slowLogEntry 是一条慢查询日志，记录日志序列号，结束时间戳，持续时间，命令
type slowLogEntry struct {
	id        int64 //
	timestamp int64
	duration  int64
	command   [][]byte
	cost      int64
}

func (entry *slowLogEntry) Cost() int64 {
	return entry.cost + slowLogEntryBasicCost
}

// ToResp 将当前 entry 转换为 resp 格式的消息
func (entry *slowLogEntry) ToResp() resp.RedisData {
	r := make([]resp.RedisData, 0, 4)
	r = append(r, resp.MakeIntData(entry.id))
	r = append(r, resp.MakeIntData(entry.timestamp))
	r = append(r, resp.MakeIntData(entry.duration))
	cmd := make([]resp.RedisData, 0, len(entry.command))
	for i := range entry.command {
		cmd = append(cmd, resp.MakeBulkData(entry.command[i]))
	}
	r = append(r, resp.MakeArrayData(cmd))
	return resp.MakeArrayData(r)
}

// slowLog 记录当前服务器中的慢查询日志
type slowLog struct {
	nid int64
	cl  *structure.CappedList
}

func newSlowLog(max int) *slowLog {
	return &slowLog{
		nid: 0,
		cl:  structure.NewCappedList(max),
	}
}

// appendEntry 追加一条慢查询日志
func (sl *slowLog) appendEntry(command [][]byte, duration int64) {

	sl.nid++

	ent := slowLogEntry{
		id:        sl.nid,
		timestamp: global.Now.Unix(),
		duration:  duration,
		command:   command,
	}

	for i := range command {
		ent.cost += int64(len(command[i]))
	}

	sl.cl.Append(&ent)
}

// getEntries 获取 limit 条慢查询日志
func (sl *slowLog) getEntries(limit int) resp.RedisData {
	ents := sl.cl.GetN(limit)

	ret := make([]resp.RedisData, 0, len(ents))

	for i := range ents {
		ret = append(ret, ents[i].(*slowLogEntry).ToResp())
	}
	return resp.MakeArrayData(ret)
}

func (sl *slowLog) clear() {
	sl.cl.Clear()
	sl.nid = 0
}

func (sl *slowLog) Cost() int64 {
	return sl.cl.Cost() + 16
}

func (sl *slowLog) Len() int64 {
	return int64(sl.cl.Size())
}
