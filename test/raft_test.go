package SurfTest

import (
	//"cse224/proj5/pkg/surfstore"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"testing"
	"time"
)

func TestRaftSetLeader(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(1) {
			t.Fatalf("Server %d should be in term %d", idx, 1)
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}

	leaderIdx = 2
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(2) {
			t.Fatalf("Server should be in term %d", 2)
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}
}

func TestUpdateTwice(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(1) {
			t.Fatalf("Server %d should be in term %d", idx, 1)
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}

	filename := "multi_file.txt"
	meta_v1, _ := LoadMetaFromMetaFile("./meta_configs/v1.meta")
	test.Clients[leaderIdx].UpdateFile(test.Context, meta_v1[filename])

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	meta_v2, _ := LoadMetaFromMetaFile("./meta_configs/v2.meta")
	test.Clients[leaderIdx].UpdateFile(test.Context, meta_v2[filename])

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	state0, _ := test.Clients[0].GetInternalState(test.Context, &emptypb.Empty{})
	if (state0.MetaMap.FileInfoMap)[filename].Version != int32(2) {
		t.Fatalf("Wrong version")
	}
	state1, _ := test.Clients[1].GetInternalState(test.Context, &emptypb.Empty{})
	if (state1.MetaMap.FileInfoMap)[filename].Version != int32(2) {
		t.Fatalf("Wrong version")
	}

}

func TestNewLeaderPushUpdates(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(1) {
			t.Fatalf("Server %d should be in term %d", idx, 1)
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	//crash
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	filename := "multi_file.txt"
	meta_v1, _ := LoadMetaFromMetaFile("./meta_configs/v1.meta")
	go test.Clients[leaderIdx].UpdateFile(test.Context, meta_v1[filename])
	time.Sleep(time.Millisecond * 5)
	test.Clients[leaderIdx].Crash(test.Context, &emptypb.Empty{})
	time.Sleep(time.Millisecond * 50)
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	leaderIdx = 1
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	t.Log("Leader 2 get a request")
	meta_v2, _ := LoadMetaFromMetaFile("./meta_configs/v2.meta")
	test.Clients[leaderIdx].UpdateFile(test.Context, meta_v2[filename])

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	t.Log("Restore server 0")
	test.Clients[0].Restore(test.Context, &emptypb.Empty{})

	t.Log("sending heartbeats")
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	state1, _ := test.Clients[1].GetInternalState(test.Context, &emptypb.Empty{})
	for i, server := range test.Clients {
		state, err := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil || err != nil {
			t.Fatalf("Fail fetching state of server %d", i)
		}
		if state != nil && !SameMeta(state1.MetaMap.FileInfoMap, state.MetaMap.FileInfoMap) {
			t.Fatalf("Incorrect File meta")
		}
		//if i == 0 && len(state.Log) != 1 {
		//	t.Fatalf("Server 0 should have log of length 1")
		//}

	}

}
