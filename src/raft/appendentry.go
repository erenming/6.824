package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// rpc handler for AppendEntries
// reset election timeout, avoid of elect as leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if rf.state == candidate {
		rf.state = follower
	}
	rf.currentTerm = args.Term

	reply.Term = rf.currentTerm
	reply.Success = true
	rf.lastApplyTime = time.Now()
}

func (rf *Raft) runHeartbeat(done chan struct{}) {
	DPrintf("heatbeat loop started!")
	for {
		select {
		case <-done:
			return
		default:
		}
		time.Sleep(rf.electionTimout / 300)
		_ = rf.parallelismAppendEntities(&AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		})
	}
}

func (rf *Raft) parallelismAppendEntities(args *AppendEntriesArgs) []AppendEntriesReply {
	replies := make([]AppendEntriesReply, 0)
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}

		r := AppendEntriesReply{}
		ok := rf.sendAppendEntities(idx, args, &r)
		if ok {
			replies = append(replies, r)
		} else {
			// DPrintf("sendAppendEntities failed")
		}
	}
	return replies
}
