package server

import (
	"fmt"
	"github.com/tangrc99/MemTable/resp"
)

func cluster(s *Server, cli *Client, cmd [][]byte) resp.RedisData {

	return resp.MakeIntData(int64(s.getSlot(string(cmd[2]))))

}

func RegisterClusterCommand() {
	RegisterCommand("cluster", cluster, RD)
}

// clusterForbiddenTable 记录集群中不允许运行的命令
var clusterForbiddenTable = map[string]struct{}{
	"keys": {},
}

// checkCommandRunnableInCluster 判断在当前的集群状态中是否允许该命令执行
func checkCommandRunnableInCluster(s *Server, cmd [][]byte) (allowed bool, err resp.RedisData) {

	if s.clusterStatus.state == ClusterNone {

		if string(cmd[0]) == "cluster" {
			return false, resp.MakeErrorData("ERR This instance has cluster support disabled")
		}

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
