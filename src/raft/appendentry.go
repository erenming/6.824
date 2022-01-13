package raft

import (
	"sync"
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
		rf.DPrintf("AppendEntries false from %d with term: %d, role: %s", args.LeaderId, args.Term, rf.role)
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
	rf.DPrintf("AppendEntries success from %d with term: %d, role: %s", args.LeaderId, args.Term, rf.role)
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastAE(args *AppendEntriesArgs) []AppendEntriesReply {
	resp := make([]AppendEntriesReply, 0)
	ch := make(chan AppendEntriesReply)
	// TODO current
	var wg sync.WaitGroup
	wg.Add(len(rf.peers) - 1)
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(server int) {
			defer wg.Done()
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, args, &reply)
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
	for reply := range ch {
		resp = append(resp, reply)
	}
	return resp
}

func (rf *Raft) handleAppendEntryReplies(args *AppendEntriesArgs, resp []AppendEntriesReply) {
	// rf.DPrintf("handleAppendEntryReplies, resp: %+v, term: %d, role: %s", resp, rf.CurrentTerm(), rf.Role())
	for _, r := range resp {
		if rf.CurrentTerm() < r.Term {
			rf.DPrintf("leader received bigger term")
			rf.toFollower(r.Term)
			return
		}
	}
}
