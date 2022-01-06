package raft

import (
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.votedFor != nil || args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	rf.votedFor = &args.CandidateId
	reply.VoteGranted = true
	reply.Term = args.Term
}

func (rf *Raft) runRequestVoteChecker() {
	rf.DPrintf("runRequestVoteChecker started")
	for {
		time.Sleep(rf.electionTimout / 10)
		if rf.getState() != leader && time.Since(rf.LastApplyTime()) >= rf.electionTimout {
			// start request vote
			rf.startElection()
		}
	}
}

func (rf *Raft) startElection() {
	rf.incTerm()
	rf.setState(candidate)
	rf.SetVotedFor(nil)

	replies := rf.parallelismRequestVote(&RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	})
	rf.handleReplies(replies)
}

func (rf *Raft) parallelismRequestVote(args *RequestVoteArgs) []RequestVoteReply {
	replies := make([]RequestVoteReply, 0)
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}

		r := RequestVoteReply{}
		ok := rf.sendRequestVote(idx, args, &r)
		if ok {
			replies = append(replies, r)
		} else {
			// DPrintf("sendRequestVote failed")
		}
	}
	return replies
}

func (rf *Raft) handleReplies(replies []RequestVoteReply) {
	winCnt := 0
	for _, r := range replies {
		if r.VoteGranted {
			winCnt++
		}
	}
	rf.DPrintf("handleReplies: %+v", replies)
	if rf.getState() == candidate && (winCnt+1)*2 > len(rf.peers) {
		rf.setState(leader)

		_ = rf.parallelismAppendEntities(&AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		})

		rf.doneHeartBeat = make(chan struct{})
		go rf.runHeartbeat(rf.doneHeartBeat)
		rf.DPrintf("after win %s", rf.getState())
		return
	}
	rf.DPrintf("not win")
}

func (rf *Raft) DPrintf(format string, a ...interface{}) (n int, err error) {
	return DPrintf(format+" === [me: %d]", append(a, rf.me)...)
}
