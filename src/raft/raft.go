package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type ServerRole string

const (
	LEADER    ServerRole = "LEADER"
	CANDIDATE ServerRole = "CANDIDATE"
	FOLLOWER  ServerRole = "FOLLOWER"
)

type LogEntry struct {
	Index   int
	Command interface{}
	Term    int
}

func (le LogEntry) Equal(other LogEntry) bool {
	return le.Term == other.Term && le.Index == other.Index
}

func (rf *Raft) Role() ServerRole {
	return rf.role.Load().(ServerRole)
}

func (rf *Raft) SetRole(role ServerRole) {
	rf.role.Store(role)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	refreshTime     time.Time
	electionTimeout time.Duration

	currentTerm, votedFor int
	role                  atomic.Value

	// doneHeartBeat chan struct{}
	// toFollowerCh  chan toFollowerEvent
	toCandidateCh chan struct{}
	toLeaderCh    chan struct{}
	notLeaderCh   chan struct{}
	doneServer    chan struct{}

	logs []LogEntry // start from index=1, ignore logs[0]

	// replicate related
	// index of highest log entry applied to state machine
	lastApplied int
	nextIndex   []int
	// commit related
	commitIndex int
	matchIndex  []int

	debugMu sync.Mutex
	applyCh chan ApplyMsg
}

type toFollowerEvent struct {
	term    int
	server  int
	traceID string
}

func (rf *Raft) NextIndex() []int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex
}

func (rf *Raft) LastApplied() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastApplied
}

func (rf *Raft) SetLastApplied(lastApplied int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastApplied = lastApplied
}

func (rf *Raft) CurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) SetCurrentTerm(currentTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = currentTerm
}

func (rf *Raft) ElectionTimeout() time.Duration {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.electionTimeout
}

func (rf *Raft) SetElectionTimeout(electionTimeout time.Duration) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimeout = electionTimeout
}

func (rf *Raft) RefreshTime() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.refreshTime
}

func (rf *Raft) SetRefreshTime(refreshTime time.Time) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.refreshTime = refreshTime
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.Role() == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	if err := d.Decode(&term); err == nil {
		rf.currentTerm = term
	} else {
		panic(err)
	}

	var votedFor int
	if err := d.Decode(&votedFor); err == nil {
		rf.votedFor = votedFor
	} else {
		panic(err)
	}

	var logs []LogEntry
	if err := d.Decode(&logs); err == nil {
		rf.logs = logs
	} else {
		panic(err)
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.doneServer)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) DPrintf(format string, a ...interface{}) (n int, err error) {
	rf.debugMu.Lock()
	defer rf.debugMu.Unlock()
	return DPrintf(format+" === [me: %d]", append(a, rf.me)...)
}

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

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.electionTimeout = randomElectionTimeout()
	rf.refreshTime = time.Now()

	rf.SetRole(FOLLOWER)
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1)
	rf.readPersist(persister.ReadRaftState())

	rf.applyCh = applyCh
	rf.lastApplied = 0
	rf.initNextIndex()
	rf.initMatchIndex()

	// rf.toFollowerCh = make(chan toFollowerEvent)
	rf.toCandidateCh = make(chan struct{})
	rf.toLeaderCh = make(chan struct{})
	rf.notLeaderCh = make(chan struct{})
	rf.doneServer = make(chan struct{})
	rf.eventLoop()

	go rf.checkElectionTimeout()

	return rf
}

func (rf *Raft) updateStateMachine(msg ApplyMsg) {
	rf.lastApplied++
	rf.applyCh <- msg
}

func (rf *Raft) checkElectionTimeout() {
	for {
		select {
		case <-rf.doneServer:
			return
		default:
		}

		time.Sleep(5 * time.Millisecond)
		rf.mu.Lock()
		to := time.Since(rf.refreshTime) > rf.electionTimeout
		rf.mu.Unlock()
		if rf.Role() == LEADER || !to {
			continue
		}
		rf.toCandidateCh <- struct{}{}
	}
}

func (rf *Raft) eventLoop() {
	// go rf.handleToFollower()
	go rf.handleToCandidate()
	go rf.handleToLeader()
}

// func (rf *Raft) handleToFollower() {
// 	for {
// 		select {
// 		case event := <-rf.toFollowerCh:
// 			rf.mu.Lock()
// 			// rf.DPrintf("[%s]to follower, term: %d, server: %d, logs: %+v", event.traceID, event.term, event.server, betterLogs(rf.logs))
// 			if rf.Role() == LEADER && rf.notLeaderCh != nil {
// 				close(rf.notLeaderCh)
// 			}
// 			rf.currentTerm = event.term
// 			rf.SetRole(FOLLOWER)
// 			rf.electionTimeout = randomElectionTimeout()
// 			rf.refreshTime = time.Now()
// 			rf.votedFor = -1
// 			rf.mu.Unlock()
// 		case <-rf.doneServer:
// 			return
// 		}
// 	}
// }

func (rf *Raft) convertToFollower(event toFollowerEvent) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// rf.DPrintf("[%s]to follower, term: %d, server: %d, logs: %+v", event.traceID, event.term, event.server, betterLogs(rf.logs))
	if rf.Role() == LEADER && rf.notLeaderCh != nil {
		close(rf.notLeaderCh)
	}
	rf.SetRole(FOLLOWER)
	rf.electionTimeout = randomElectionTimeout()
	rf.refreshTime = time.Now()
	rf.currentTerm = event.term
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) handleToCandidate() {
	for {
		select {
		case <-rf.doneServer:
			return
		case <-rf.toCandidateCh:
			switch rf.Role() {
			case FOLLOWER, CANDIDATE:
				// timeout, start election
				// timeout, new election
				rf.SetRefreshTime(time.Now())
				rf.SetElectionTimeout(randomElectionTimeout())
				rf.runElection()
			case LEADER:
				// impossible, then pass
			}
		}
	}
}

func (rf *Raft) handleToLeader() {
	for {
		select {
		case <-rf.doneServer:
			return
		case <-rf.toLeaderCh:
			switch rf.Role() {
			case LEADER, FOLLOWER:
				// impossible, then pass
			case CANDIDATE:
				rf.mu.Lock()
				rf.SetRole(LEADER)
				rf.notLeaderCh = make(chan struct{})
				rf.mu.Unlock()
				rf.initNextIndex()
				rf.initMatchIndex()
				go rf.runHeartBeat()
				rf.DPrintf("to leader done, term: %d", rf.CurrentTerm())
			}
		}
	}
}

func (rf *Raft) initNextIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	nextIndex := make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		nextIndex[i] = len(rf.logs)
	}
	rf.nextIndex = nextIndex
}

func (rf *Raft) initMatchIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	matchIndex := make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		matchIndex[i] = len(rf.logs) - 1
	}
	rf.matchIndex = matchIndex
}

func (rf *Raft) runHeartBeat() {
	for {
		if rf.Role() != LEADER {
			break
		}
		if rf.killed() {
			break
		}
		time.Sleep(rf.ElectionTimeout() / 10)
		rf.broadcastAppendRPC(false)
	}
}
