package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term     int
	LeaderId int
	// TODO log field
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = args.Term
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	rf.role = Follower
	rf.votedFor = -1
	rf.electionTimeout = randomElectionTimeout()
	rf.refreshTime = time.Now()

	reply.Term = args.Term
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastAE() []AppendEntriesReply {
	resp := make([]AppendEntriesReply, 0)
	args := &AppendEntriesArgs{
		Term:     rf.CurrentTerm(),
		LeaderId: rf.me,
	}
	// TODO current
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(idx, args, &reply)
		if !ok {
			continue
		}
		resp = append(resp, reply)
	}
	return resp
}

func (rf *Raft) handleAppendEntryReplies(resp []AppendEntriesReply) {
	// rf.DPrintf("handleAppendEntryReplies, resp: %+v, term: %d", resp, rf.CurrentTerm())
	for _, r := range resp {
		if r.Term > rf.CurrentTerm() {
			rf.SetCurrentTerm(r.Term)
			rf.becomeFollower <- struct{}{}
			return
		}
	}
}
