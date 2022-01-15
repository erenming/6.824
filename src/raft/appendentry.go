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


type AppendEntriesReply struct {
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
	if rf.role != Follower {
		close(rf.doneHeartBeat)
	}
	rf.role = Follower
	rf.refreshTime = time.Now()

	reply.Term = args.Term
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastAE(args *AppendEntriesArgs) {
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(server int) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, args, &reply)
			if !ok {
				return
			}
		}(idx)
	}
}

func (rf *Raft) handleAppendEntryReplies(args *AppendEntriesArgs, resp []AppendEntriesReply) {
	for _, r := range resp {
		if rf.CurrentTerm() < r.Term {
			rf.DPrintf("leader received bigger term")
			rf.toFollower(r.Term)
			return
		}
	}
}
