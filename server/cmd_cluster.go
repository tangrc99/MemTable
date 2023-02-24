package server

import (
	"fmt"
	"github.com/tangrc99/MemTable/resp"
	"strconv"
	"strings"
)

func cluster(s *Server, _ *Client, cmd [][]byte) resp.RedisData {

	// 进行输入类型检查
	e, ok := CheckCommandAndLength(&cmd, "cluster", 2)
	if !ok {
		return e
	}

	if s.clusterStatus.state == ClusterNone {
		return resp.MakeErrorData("ERR This instance has cluster support disabled")
	}

	switch strings.ToLower(string(cmd[1])) {

	case "keyslot":
		return clusterKeySlot(s, cmd)

	case "countkeysinslot":
		return clusterCountKeysInSlot(s, cmd)

	case "getkeysinslot":
		return clusterGetKeysInSlot(s, cmd)
	case "nodes":
		return clusterNodes(s, cmd)
	}

	return resp.MakeIntData(int64(s.getSlot(string(cmd[2]))))

}

func RegisterClusterCommand() {
	RegisterCommand("cluster", cluster, RD)
}

// clusterForbiddenTable 记录集群中不允许运行的命令
var clusterForbiddenTable = map[string]struct{}{
	"keys": {}, "select": {}, "mget": {}, "mset": {}, "randomkey": {},
}

func clusterInfo() {}

func clusterNodes(s *Server, cmd [][]byte) resp.RedisData {

	nodes := s.clusterStatus.nodes

	ret := make([]resp.RedisData, 0, len(nodes))
	for k := range nodes {
		ret = append(ret, resp.MakeBulkData([]byte(k)))
	}
	return resp.MakeArrayData(ret)
}

func clusterKeySlot(s *Server, cmd [][]byte) resp.RedisData {

	if len(cmd) < 3 {
		return resp.MakeErrorData(fmt.Sprintf("ERR wrong number of arguments for '%s' command", (cmd)[1]))
	}

	return resp.MakeIntData(int64(s.getSlot(string(cmd[2]))))
}

func clusterCountKeysInSlot(s *Server, cmd [][]byte) resp.RedisData {

	if len(cmd) < 3 {
		return resp.MakeErrorData(fmt.Sprintf("ERR wrong number of arguments for '%s' command", (cmd)[1]))
	}

	slotSeq, err := strconv.Atoi(string(cmd[2]))

	if err != nil || slotSeq < 0 || slotSeq > slotNum {
		return resp.MakeErrorData("ERR slot is not an integer or out of range")
	}

	slotOwner := s.clusterStatus.slots[slotSeq]

	if slotOwner != s.clusterStatus.self {
		return resp.MakeErrorData(fmt.Sprintf("MOVED %d %s", slotSeq, slotOwner.name))
	}

	return resp.MakeIntData(int64(s.dbs[0].SlotCount(slotSeq)))
}

func clusterGetKeysInSlot(s *Server, cmd [][]byte) resp.RedisData {

	if len(cmd) < 3 {
		return resp.MakeErrorData(fmt.Sprintf("ERR wrong number of arguments for '%s' command", (cmd)[1]))
	}

	slotSeq, err := strconv.Atoi(string(cmd[2]))

	if err != nil || slotSeq < 0 || slotSeq > slotNum {
		return resp.MakeErrorData("ERR slot is not an integer or out of range")
	}

	slotOwner := s.clusterStatus.slots[slotSeq]

	if slotOwner != s.clusterStatus.self {
		return resp.MakeErrorData(fmt.Sprintf("MOVED %d %s", slotSeq, slotOwner.name))
	}
	count := 1 << 32
	if len(cmd) == 4 {
		count, err = strconv.Atoi(string(cmd[2]))

		if err != nil || slotSeq < 0 {
			return resp.MakeErrorData("ERR count is not an integer or out of range")
		}
	}

	keys, nums := s.dbs[0].KeysInSlot(slotSeq, count)

	ret := make([]resp.RedisData, nums)
	for i := 0; i < nums; i++ {
		ret[i] = resp.MakeBulkData([]byte(keys[i]))
	}
	return resp.MakeArrayData(ret)
}

/* ---------------------------------------------------------------------------
* utils 函数
* ------------------------------------------------------------------------- */

// checkCommandRunnableInCluster 判断在当前的集群状态中是否允许该命令执行
func checkCommandRunnableInCluster(s *Server, cmd [][]byte) (allowed bool, err resp.RedisData) {

	if s.clusterStatus.state == ClusterNone {

		return true, nil

	} else if s.clusterStatus.state == ClusterInit {
		return false, resp.MakeErrorData("ERR This instance is being initialized")
	}

	if _, exist := clusterForbiddenTable[string(cmd[0])]; exist {

		return false, resp.MakeErrorData(fmt.Sprintf("ERR %s is not permitted in cluster", string(cmd[0])))
	}

	moved, err := checkKeyNeedsMoved(s, cmd)

	return !moved, err
}

// checkKeyNeedsMoved 用来判断命令是否需要迁移到其他实例上
func checkKeyNeedsMoved(s *Server, cmd [][]byte) (needMove bool, err resp.RedisData) {

	command := string(cmd[0])

	// 首先判断命令是否是数据库命令
	if _, exist := CommandTable[command]; !exist {
		if len(cmd) > 1 {
			if moved, slot, peer := s.isKeyNeedMove(string(cmd[1])); moved {
				return true, resp.MakeErrorData(fmt.Sprintf("MOVED %d %s", slot, peer.name))
			}
		}
	}

	return false, nil
}

func checkAllKeysLocal(s *Server, keys [][]byte, num int) bool {

	for i := 0; i < num; i++ {

		moved, _, _ := s.isKeyNeedMove(string(keys[i]))
		if moved {
			return false
		}

	}
	return true
}
