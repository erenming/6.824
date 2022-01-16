package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term                      int
	LeaderId                  int
	PrevLogIndex, PrevLogTerm int // ?
	Entries                   []LogEntry
	LeaderCommit              int
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
	if rf.role != Follower && rf.doneHeartBeat != nil {
		close(rf.doneHeartBeat)
	}
	rf.role = Follower
	rf.refreshTime = time.Now()

	rf.DPrintf("args: %+v, commitIndex: %d", args, rf.commitIndex)
	if len(args.Entries) > 0 {
		rf.logs = append(rf.logs, args.Entries...)
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			rf.updateStateMachine(ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[i].Command,
				CommandIndex: i,
			})
		}
		rf.lastApplied = rf.commitIndex
	}
	reply.Term = args.Term
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastAE() {
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			logs := []LogEntry{}
			if rf.lastApplied >= rf.nextIndex[server] {
				logs = rf.logs[rf.nextIndex[server]:]
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				Entries:      logs,
			}
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, args, &reply)
			if !ok {
				return
			}
			if reply.Success {
				rf.mu.Lock()
				rf.nextIndex[server] = len(rf.logs)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				rf.mu.Unlock()
			}
		}(idx)
	}
}
