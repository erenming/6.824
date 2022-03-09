package raft

import (
	"math"
	"sync"
)

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.Role() != LEADER {
		return -1, -1, false
	}

	// 1. apply
	rf.mu.Lock()
	le := LogEntry{
		Index:   len(rf.logs),
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.logs = append(rf.logs, le)
	latestIndex := len(rf.logs) - 1
	rf.mu.Unlock()

	// 2. replica logEntry to followers
	ch := rf.replica()
	srvList := []int{}

	// 2. handle Request&Response
	cnt, n := 1, len(rf.peers)
	for {
		if rf.Role() != LEADER {
			return -1, -1, false
		}

		select {
		case srvID, ok := <-ch:
			if !ok {
				return latestIndex, rf.CurrentTerm(), true
			}

			cnt++
			srvList = append(srvList, srvID)
			if cnt >= n/2+n%2 { // majority
				rf.checkAndCommit(srvList)
				return latestIndex, rf.CurrentTerm(), true
			}
		case <-rf.doneServer:
			return latestIndex, rf.CurrentTerm(), true
		}
	}
}

func (rf *Raft) checkAndCommit(srvList []int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex == len(rf.logs)-1 {
		return
	}
	newCommitIndex := math.MaxInt64
	for _, srv := range srvList {
		if rf.matchIndex[srv] < newCommitIndex {
			newCommitIndex = rf.matchIndex[srv]
		}
	}
	for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
		rf.updateStateMachine(ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		})
	}
	rf.commitIndex = newCommitIndex
}

// 信号通道：一旦有server复制成功则发送一个信号
func (rf *Raft) replica() chan int {
	rf.mu.Lock()
	rf.DPrintf("start replica, prevLog: %+v, cidx: %d", rf.logs[len(rf.logs)-1], rf.commitIndex)
	rf.mu.Unlock()
	ch := make(chan int, len(rf.peers)-1)
	var wg sync.WaitGroup
	wg.Add(len(rf.peers) - 1)
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}

		go func(server int) {
			defer wg.Done()
			ok := rf.replicaServer(server, ch)
			if !ok {
				// network error, retry background forever
				// go func() {
				// 	for !rf.replicaServer(server, nil) {
				// 		// decrease cpu
				// 		time.Sleep(time.Millisecond * 100)
				// 	}
				// }()
				return
			}

		}(idx)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}

func (rf *Raft) replicaServer(srvID int, echoCh chan int) bool {
redo:
	if rf.killed() || rf.Role() != LEADER {
		return true
	}
	rf.mu.Lock()
	if len(rf.logs) < rf.nextIndex[srvID] {
		rf.mu.Unlock()
		return true
	}
	logs := rf.logs[rf.nextIndex[srvID]:]
	prevLog := rf.logs[rf.nextIndex[srvID]-1]
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		Entries:      logs,
		PrevLogIndex: prevLog.Index,
		PrevLogTerm:  prevLog.Term,
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(srvID, args, &reply)
	if !ok {
		return false
	}

	if reply.Term > rf.CurrentTerm() {
		rf.toFollowerCh <- toFollowerEvent{
			term:   reply.Term,
			server: rf.me,
		}
		return true
	}

	if reply.Success {
		rf.mu.Lock()
		toInc := 0
		for i := len(logs) - 1; i >= 0; i-- {
			if logs[i].Index >= rf.nextIndex[srvID] {
				toInc++
			}
		}
		rf.nextIndex[srvID] += toInc
		rf.matchIndex[srvID] += toInc
		rf.mu.Unlock()
		rf.DPrintf("reply.Success, server: %d, add: %d", srvID, toInc)
		if echoCh != nil {
			echoCh <- srvID
		}
		return true
	} else {
		// handle rejected AppendRPC
		rf.mu.Lock()
		rf.nextIndex[srvID]--
		rf.matchIndex[srvID]--
		rf.mu.Unlock()
		rf.DPrintf("reply.Fail, server: %d", srvID)
		goto redo
	}
}
