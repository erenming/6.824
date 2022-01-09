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
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type ServerRole string

const (
	leader    ServerRole = "leader"
	candidate ServerRole = "candidate"
	follower  ServerRole = "follower"
)

type LogEntry struct {
	termId  int
	command interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	lastApplyTime   time.Time
	doneHeartBeat   chan struct{}
	electionEvent   chan struct{}
	asLeaderEvent   chan struct{}
	asFollowerEvent chan followerEvent
	role            ServerRole

	currentTerm      int
	votedFor         *int
	asCandidateEvent chan struct{}

	// logEntries  []LogEntry
	// commitIndex int
	// lastApplied int
	//
	// nextIndex  []int
	// matchIndex []int
}

func (rf *Raft) Role() ServerRole {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

func (rf *Raft) SetRole(role ServerRole) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = role
}

func (rf *Raft) VotedFor() *int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
}

func (rf *Raft) SetVotedFor(votedFor *int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = votedFor
}

func (rf *Raft) LastApplyTime() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastApplyTime
}

func (rf *Raft) SetLastApplyTime(lastApplyTime time.Time) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastApplyTime = lastApplyTime
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == leader
}

func (rf *Raft) CurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}
func (rf *Raft) SetCurrentTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
}

func (rf *Raft) incTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntities(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		doneHeartBeat:    make(chan struct{}),
		electionEvent:    make(chan struct{}),
		asLeaderEvent:    make(chan struct{}),
		asCandidateEvent: make(chan struct{}),
		asFollowerEvent:  make(chan followerEvent),
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = follower
	rf.lastApplyTime = time.Now()
	go rf.runRequestVoteChecker()
	go rf.ElectionEventLoop()
	go rf.roleEventLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
