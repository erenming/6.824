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
	rf.electionTimeout = randomElectionTimeout()
	rf.refreshTime = time.Now()

	reply.Term = args.Term
	reply.Success = true
	// rf.DPrintf("AppendEntries success, role: %s", rf.role)
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastAE(args *AppendEntriesArgs) []AppendEntriesReply {
	resp := make([]AppendEntriesReply, 0)

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

func (rf *Raft) handleAppendEntryReplies(args *AppendEntriesArgs, resp []AppendEntriesReply) {
	// rf.DPrintf("handleAppendEntryReplies, resp: %+v, term: %d, role: %s", resp, rf.CurrentTerm(), rf.Role())
	for _, r := range resp {
		if r.Term > args.Term {
			rf.becomeFollower <- r.Term
			return
		}
	}
}
