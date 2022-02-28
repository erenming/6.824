package raft

import (
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
	length := len(rf.logs)
	rf.mu.Unlock()

	// 2. replica logEntry to followers
	ch := rf.replica()

	// 2. handle Request&Response
	cnt, n := 1, len(rf.peers)
	for {
		if rf.Role() != LEADER {
			return -1, -1, false
		}

		select {
		case l, ok := <-ch:
			if !ok {
				return latestIndex, rf.CurrentTerm(), true
			}

			if l < length {
				length = l
			}

			cnt++
			if cnt >= n/2+n%2 { // majority
				rf.checkAndApply(length)
				return latestIndex, rf.CurrentTerm(), true
			}
		}
	}
}

func (rf *Raft) checkAndApply(length int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// match := make([]int, len(rf.matchIndex))
	// copy(match, rf.matchIndex)
	// sort.Slice(match, func(i, j int) bool {
	// 	return match[i] > match[j]
	// })
	//
	// majority := len(rf.peers)/2 + len(rf.peers)%2
	// toCommit := match[0]
	// for i := 1; i < len(rf.nextIndex); i++ {
	// 	if rf.nextIndex[i] == toCommit {
	// 		continue
	// 	}
	// 	if i-1 >= majority {
	// 		break
	// 	} else {
	// 		toCommit = rf.nextIndex[i]
	// 	}
	// }

	for i := rf.commitIndex + 1; i < length+rf.commitIndex+1; i++ {
		rf.updateStateMachine(ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		})
	}
	rf.commitIndex += length
}

// 信号通道：一旦有server复制成功则发送一个信号
func (rf *Raft) replica() chan int {
	ch := make(chan int)
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
				// // network error, retry background forever
				// go func() {
				// 	for !rf.replicaServer(server, nil) {
				// 		// decrease cpu
				// 		time.Sleep(time.Millisecond * 10)
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
	if rf.killed() {
		return true
	}
	rf.mu.Lock()
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
		rf.toFollowerCh <- reply.Term
		return true
	}

	if reply.Success {
		rf.mu.Lock()
		rf.nextIndex[srvID] += len(logs)
		rf.matchIndex[srvID] += len(logs)
		rf.mu.Unlock()
		if echoCh != nil {
			echoCh <- len(logs)
		}
		return true
	} else {
		// handle rejected AppendRPC
		rf.mu.Lock()
		rf.nextIndex[srvID]--
		rf.mu.Unlock()
		goto redo
	}
}
