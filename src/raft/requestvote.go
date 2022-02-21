package raft

import (
	"sync"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term                      int
	CandidateID               int
	LastLogTerm, LastLogIndex int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if args.Term > rf.CurrentTerm() {
		rf.toFollowerCh <- args.Term
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isMoreUpToDate(args) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else {
		rf.DPrintf("args: %+v is more up-to-date, prevLog: %+v", args, rf.logs[rf.lastApplied])
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		rf.votedFor = args.CandidateID
		rf.electionTimeout = randomElectionTimeout()
		rf.refreshTime = time.Now()
		reply.Term = args.Term
		reply.VoteGranted = true
	} else {
		reply.Term = args.Term
		reply.VoteGranted = false
	}
}

// check args is more update-to-date server
func (rf *Raft) isMoreUpToDate(args *RequestVoteArgs) bool {
	prevLog := rf.logs[rf.commitIndex]
	if args.LastLogTerm == prevLog.Term {
		return args.LastLogIndex >= prevLog.Index
	} else {
		return args.LastLogTerm > prevLog.Term
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) runElection() {
	rf.mu.Lock()
	rf.role = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	lastLog := rf.logs[rf.commitIndex]
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogTerm:  lastLog.Term,
		LastLogIndex: lastLog.Index,
	}
	rf.mu.Unlock()

	ch := rf.broadcastRV(args)
	cnt, n := 1, len(rf.peers)
	for {
		select {
		case <-time.After(rf.ElectionTimeout()):
			return
		case reply := <-ch:
			if args.Term != reply.Term || rf.CurrentTerm() != reply.Term {
				continue
			}

			if reply.VoteGranted {
				cnt++
			}

			if cnt >= n/2+n%2 && rf.Role() == CANDIDATE {
				rf.toLeaderCh <- struct{}{}
				return
			}
		}
	}

}

func (rf *Raft) broadcastRV(args *RequestVoteArgs) chan RequestVoteReply {
	ch := make(chan RequestVoteReply)
	var wg sync.WaitGroup
	wg.Add(len(rf.peers) - 1)
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(server int) {
			defer wg.Done()
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, &reply)
			if !ok {
				return
			}
			ch <- reply
		}(idx)

	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}
