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
	if rf.role != FOLLOWER && rf.doneHeartBeat != nil {
		close(rf.doneHeartBeat)
	}
	rf.role = FOLLOWER
	rf.refreshTime = time.Now()

	if len(args.Entries) > 0 {
		prevLog := rf.logs[len(rf.logs)-1]
		if prevLog.Term != args.PrevLogTerm || prevLog.Index != args.PrevLogIndex {
			reply.Term = args.Term
			reply.Success = false
			return
		}
		rf.DPrintf("append entries, args: %+v", args)
		rf.logs = append(rf.logs, args.Entries...)
		rf.lastApplied = len(rf.logs)-1
	}

	if args.LeaderCommit > rf.commitIndex {
		for i := rf.commitIndex + 1; i <= rf.lastApplied; i++ {
			rf.updateStateMachine(ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[i].Command,
				CommandIndex: i,
			})
		}
		rf.commitIndex = rf.lastApplied
	}
	reply.Term = args.Term
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
