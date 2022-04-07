package raft

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

	rf.mu.Lock()
	le := LogEntry{
		Index:   len(rf.logs),
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.logs = append(rf.logs, le)
	latestIndex := len(rf.logs) - 1
	rf.mu.Unlock()

	go rf.broadcastAppendRPC(true)
	return latestIndex, rf.CurrentTerm(), true
}

func (rf *Raft) checkAndCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	m := make(map[int]int)
	for _, matchedIdx := range rf.matchIndex {
		m[matchedIdx]++
	}
	toCommit := -1
	for k, v := range m {
		if isMajority(v, len(rf.peers)) && k > toCommit {
			toCommit = k
		}
	}
	if toCommit <= rf.commitIndex {
		return
	}
	if rf.logs[toCommit].Term != rf.currentTerm {
		return
	}
	for i := rf.commitIndex + 1; i <= toCommit; i++ {
		rf.updateStateMachine(ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		})
	}
	rf.commitIndex = toCommit
}

func isMajority(cnt int, total int) bool {
	cnt++ // include leader self
	return cnt >= total/2+total%2
}
