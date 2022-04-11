package raft

import (
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term                      int
	CandidateID               int
	LastLogTerm, LastLogIndex int
	TraceID                   string
}

type RequestVoteReply struct {
	TraceID     string
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.TraceID = args.TraceID

	if args.Term > rf.CurrentTerm() {
		rf.convertToFollower(toFollowerEvent{
			term:    args.Term,
			server:  rf.me,
			traceID: args.TraceID,
		})
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.isMoreUpToDate(args) {
		rf.DPrintf("[%s]!isMoreUpToDate", args.TraceID)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term < rf.currentTerm {
		rf.DPrintf("[%s]args.Term < rf.currentTerm", args.TraceID)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		rf.DPrintf("[%s]voted success!!!", args.TraceID)
		rf.votedFor = args.CandidateID
		rf.persist()
		rf.electionTimeout = randomElectionTimeout()
		rf.refreshTime = time.Now()
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else {
		rf.DPrintf("[%s]ended rejected", args.TraceID)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

// check args is more update-to-date server
func (rf *Raft) isMoreUpToDate(args *RequestVoteArgs) bool {
	prevLog := rf.logs[len(rf.logs)-1]
	if args.LastLogTerm == prevLog.Term {
		return args.LastLogIndex >= prevLog.Index
	} else {
		return args.LastLogTerm >= prevLog.Term
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) runElection() {
	rf.mu.Lock()
	rf.SetRole(CANDIDATE)
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	lastLog := rf.logs[len(rf.logs)-1]
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogTerm:  lastLog.Term,
		LastLogIndex: lastLog.Index,
		TraceID:      RandStringBytes(),
	}
	rf.mu.Unlock()
	rf.DPrintf("[%s]Vote %d, %d", args.TraceID, args.CandidateID, args.Term)

	ch := rf.broadcastRV(args)
	cnt, n := 1, len(rf.peers)
	for {
		select {
		case <-rf.doneServer:
			return
		case <-time.After(rf.ElectionTimeout()):
			rf.mu.Lock()
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()
			rf.DPrintf("[%s]ElectionTimeout", args.TraceID)
			return
		case reply, ok := <-ch:
			if !ok {
				rf.DPrintf("[%s] closed ch", args.TraceID)
				return
			}
			if reply.Term > rf.CurrentTerm() {
				rf.convertToFollower(toFollowerEvent{
					term:    reply.Term,
					server:  rf.me,
					traceID: args.TraceID,
				})
				return
			}
			if reply.Term > args.Term {
				return
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
	ch := make(chan RequestVoteReply, len(rf.peers)-1)
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(server int) {
			// defer wg.Done()
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, &reply)
			if !ok {
				return
			}
			ch <- reply
		}(idx)

	}
	return ch
}
