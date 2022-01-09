package raft

import (
	"math"
	"sync"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	// LastLogIndex int
	// LastLogTerm  int
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
	if rf.votedFor != nil {
		rf.DPrintf("args: %+v, myterm: %d, rf.votedFor: %d", args, rf.currentTerm, *rf.votedFor)
	} else {
		rf.DPrintf("args: %+v, myterm: %d, rf.votedFor: nil", args, rf.currentTerm)
	}

	if rf.currentTerm > args.Term || rf.role == leader {
		// rf.DPrintf("RequestVote reject, args.Term: %d, rf.currentTerm: %d", args.Term, rf.currentTerm)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = &args.CandidateId
		reply.VoteGranted = true
		reply.Term = args.Term
		return
	}

	if rf.votedFor == nil || rf.votedFor == &args.CandidateId {
		rf.votedFor = &args.CandidateId
		reply.VoteGranted = true
		reply.Term = args.Term
	} else {
		reply.VoteGranted = false
		reply.Term = args.Term
	}
	// rf.DPrintf("send back requestVote: %+v", reply)
}

func (rf *Raft) broadcastRequestVote() []RequestVoteReply {
	args := &RequestVoteArgs{
		Term:        rf.CurrentTerm(),
		CandidateId: rf.me,
	}
	ch := make(chan RequestVoteReply)
	replies := make([]RequestVoteReply, 0)

	var wg sync.WaitGroup
	wg.Add(len(rf.peers) - 1)
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(peer int) {
			defer wg.Done()
			r := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, args, &r)
			if ok {
				// rf.DPrintf("send to server: %d, currentTerm: %d, reply ok: %+v", idx, rf.CurrentTerm(), r)
				ch <- r
			} else {
				// rf.DPrintf("send to server: %d, currentTerm: %d, reply not ok: %+v", idx, rf.CurrentTerm(), r)
			}
		}(idx)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	for r := range ch {
		replies = append(replies, r)
	}
	return replies

}

func (rf *Raft) handleRequestVoteReplies(replies []RequestVoteReply) {
	rf.DPrintf("replies: %+v, currentTerm: %d", replies, rf.CurrentTerm())
	for _, r := range replies {
		if r.Term > rf.CurrentTerm() {
			rf.asFollowerEvent <- followerEvent{Term: r.Term}
			return
		}
	}

	winCnt := 0
	for _, r := range replies {
		if r.VoteGranted {
			winCnt++
		}
	}
	half := int(math.Ceil(float64(len(rf.peers)) / 2.0))
	if rf.Role() == candidate && (winCnt+1) >= half {
		rf.DPrintf("win, send asLeaderEvent")
		rf.asLeaderEvent <- struct{}{}
		rf.DPrintf("win, send asLeaderEvent end")
	}
}

func (rf *Raft) DPrintf(format string, a ...interface{}) (n int, err error) {
	return DPrintf(format+" === [me: %d]", append(a, rf.me)...)
}
