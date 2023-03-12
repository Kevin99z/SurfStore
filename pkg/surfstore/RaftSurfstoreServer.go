package surfstore

import (
	context "context"
	"fmt"
	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"sync"
)

type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation
	id            int64
	raftAddrs     []string
	commitIndex   int64
	lastApplied   int64
	nextIndex     []int64
	matchIndex    []int64

	metaStore *MetaStore

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	fmt.Printf("[Server %d] GetFileInfoMap\n", s.id)
	success := false
	for !success {
		res, _ := s.SendHeartbeat(ctx, nil)
		success = success || res.Flag
	}
	return s.metaStore.GetFileInfoMap(ctx, empty)
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	success := false
	for !success {
		res, _ := s.SendHeartbeat(ctx, nil)
		success = success || res.Flag
	}
	return s.metaStore.GetBlockStoreMap(ctx, hashes)
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	success := false
	for !success {
		res, _ := s.SendHeartbeat(ctx, nil)
		success = success || res.Flag
	}
	return s.metaStore.GetBlockStoreAddrs(ctx, empty)
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	ok, err := s.CheckIsValidLeader()
	if !ok {
		return nil, err
	}
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})
	fmt.Printf("[Server %d] UpdateFile (new log appended)\n", s.id)
	success := false
	for !success {
		res, err := s.SendHeartbeat(ctx, nil)
		if err != nil {
			fmt.Printf("[Server %d] UpdateFile exit because %s\n", s.id, err)
			return nil, err
		}
		if res != nil {
			success = success || res.Flag
		}
	}
	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied]
		filemeta := entry.FileMetaData
		fmt.Printf("[Server %d] Commit %s (version %d)\n", s.id, filemeta.Filename, filemeta.Version)
		s.metaStore.UpdateFile(ctx, filemeta)
	}
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, nil)
	fileMetaData, ok := fileInfoMap.FileInfoMap[filemeta.Filename]
	version := int32(-1)
	if ok {
		version = fileMetaData.Version
	}
	return &Version{Version: version}, nil
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)

func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	if input.Term < s.term {
		return &AppendEntryOutput{
			ServerId: s.id,
			Term:     s.term,
			Success:  false,
		}, nil
	} else {
		s.term = input.Term
		s.isLeader = false
	}
	fmt.Printf("[Server %d] AppendEntries\n", s.id)

	if len(s.log) <= int(input.PrevLogIndex) || (input.PrevLogIndex >= 0 && s.log[input.PrevLogIndex].Term != input.PrevLogTerm) {
		return &AppendEntryOutput{
			ServerId: s.id,
			Term:     s.term,
			Success:  false,
		}, nil
	}

	s.log = append(s.log[:input.PrevLogIndex+1], input.Entries...)

	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(min(int(input.LeaderCommit), len(s.log)-1))
	}

	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied]
		filemeta := entry.FileMetaData
		fmt.Printf("[Server %d] Commit %s (version %d)\n", s.id, filemeta.Filename, filemeta.Version)
		s.metaStore.UpdateFile(ctx, filemeta)
	}

	return &AppendEntryOutput{
		ServerId:     s.id,
		Term:         s.term,
		Success:      true,
		MatchedIndex: int64(len(s.log) - 1),
	}, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Printf("[Server %d] Set as Leader\n", s.id)
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.term += 1
	for i := 0; i < len(s.raftAddrs); i++ {
		//if int64(i) == s.id {
		//	continue
		//}
		s.nextIndex[i] = s.commitIndex + 1
		s.matchIndex[i] = -1
	}
	s.isLeaderMutex.Unlock()
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) CheckIsValidLeader() (bool, error) {
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return false, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return false, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()
	return true, nil
}
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	//fmt.Println("Sending heartbeat")
	succCnt := 0
	for i, addr := range s.raftAddrs {
		ok, err := s.CheckIsValidLeader()
		if !ok {
			return &Success{Flag: false}, err
		}
		fmt.Printf("[Server %d] Sending heartbeat to server %d\n", s.id, i)
		if int64(i) == s.id {
			continue
		}
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			fmt.Printf("Failed dialing server\n")
			continue
		}
		c := NewRaftSurfstoreClient(conn)
		finished := false
		for !finished {
			ok, err := s.CheckIsValidLeader()
			if !ok {
				return &Success{Flag: false}, err
			}
			prevLogIdx := s.nextIndex[i] - 1
			var prevLogTerm int64
			if prevLogIdx >= 0 {
				prevLogTerm = s.log[prevLogIdx].Term
			}

			resp, err := c.AppendEntries(ctx, &AppendEntryInput{
				Term:         s.term,
				PrevLogIndex: prevLogIdx,
				PrevLogTerm:  prevLogTerm,
				Entries:      s.log[prevLogIdx+1:],
				LeaderCommit: s.commitIndex,
			})
			if err == nil {
				if resp.Success {
					//fmt.Printf("[Server %d] server %d is alive\n", s.id, i)
					succCnt += 1
					s.matchIndex[i] = resp.MatchedIndex
					finished = true
				} else if resp.Term > s.term {
					s.isLeaderMutex.Lock()
					s.isLeader = false
					s.isLeaderMutex.Unlock()
					return &Success{Flag: false}, ERR_NOT_LEADER
				} else {
					s.nextIndex[i] -= 1
					continue
				}
			} else {
				break
			}
		}
	}

	for N := int64(len(s.log) - 1); N > s.commitIndex; N-- {
		cnt := 0
		for id, idx := range s.matchIndex {
			fmt.Printf("id:%d, matchIndex: %d", id, idx)
			if idx >= N {
				cnt += 1
			}
		}
		if cnt > len(s.raftAddrs)/2-1 {
			fmt.Printf("[Server %d]cnt=%d, Update commitIndex to %d\n", s.id, cnt, N)
			s.commitIndex = N
			break
		}
	}

	return &Success{Flag: succCnt > len(s.raftAddrs)/2-1}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()
	fmt.Printf("[server %d] Crashed\n", s.id)
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()
	fmt.Printf("[server %d] Restored\n", s.id)
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	fmt.Printf("[Server %d] server has %d logs\n", s.id, len(s.log))
	//for _, op := range s.log {
	//	filemeta := op.FileMetaData
	//	fmt.Println("\t", filemeta.Filename, filemeta.Version)
	//}
	PrintMetaMap(fileInfoMap.FileInfoMap)
	fmt.Println()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
