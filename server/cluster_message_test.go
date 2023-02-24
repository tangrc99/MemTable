package server

import "testing"

func TestClusterMessage(t *testing.T) {

	if generateNewLeaderMessage(0, "0.0.0.0:6380") != ""+
		"{\"shard\":0,\"type\":0,\"content\":\"0.0.0.0:6380\"}" {
		t.Error("Message Generate Failed")
	}

}

func TestClusterJson(t *testing.T) {

	n2 := clusterNode{
		name: "0.0.0.0:6390",
	}
	n1 := clusterNode{
		name:   "0.0.0.0:6379",
		slaves: []*clusterNode{&n2},
	}
	n1.slaveOf = &n1

	if string(n1.toJson()) != "{\"name\":\"0.0.0.0:6379\",\"slaves\":[\"0.0.0.0:6390\"],\"slave_of\":\"0.0.0.0:6379\"}" {
		t.Error("ClusterNode to json Failed With: ")
		println(string(n1.toJson()))

	}

	n3 := clusterNode{
		name: "0.0.0.0:6379",
	}
	n3.slaveOf = &n3

	sts := clusterStatus{
		state:         ClusterOK,
		nodes:         map[string]*clusterNode{n1.name: &n1, n2.name: &n2},
		configNodeNum: 2,
		config: clusterConfig{
			ClusterName: "cluster_000",
			ShardNum:    2,
			Shards: [][]string{
				{"0.0.0.0:6379", "0.0.0.0:6390"},
			},
		},
	}

	if string(sts.toJson()) != "{\"config\":{\"cluster_name\":\"cluster_000\",\"shard_num\":2,\"shards\":[[\"0.0.0.0:6379\",\"0.0.0.0:6390\"]]},"+
		"\"alive_nodes\":2,"+
		"\"state\":2,"+
		"\"masters\":[{\"name\":\"0.0.0.0:6379\",\"slaves\":[\"0.0.0.0:6390\"],\"slave_of\":\"0.0.0.0:6379\"}]}" {
		t.Error("ClusterStatus to json Failed With: ")
		println(string(sts.toJson()))
	}
}
