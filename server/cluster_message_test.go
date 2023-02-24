package server

import "testing"

func TestClusterMessage(t *testing.T) {

	if generateNewLeaderMessage(0, "0.0.0.0:6380") != ""+
		"{\"shard\":0,\"type\":0,\"content\":\"0.0.0.0:6380\"}" {
		t.Error("Message Generate Failed")
	}

}
