package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term                      int
	LeaderId                  int
	PrevLogIndex, PrevLogTerm int
	Entries                   []LogEntry
	LeaderCommit              int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	if args.Term < rf.CurrentTerm() {
		reply.Term = rf.CurrentTerm()
		reply.Success = false
		return
	}

	if rf.Role() != FOLLOWER {
		rf.toFollowerCh <- toFollowerEvent{
			term:   args.Term,
			server: rf.me,
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = args.Term
	rf.refreshTime = time.Now()

	// it's a log replica when len(args.entries) > 0, else a heartbeat
	// if len(args.Entries) > 0 {
	lastLog := rf.logs[len(rf.logs)-1]
	if args.PrevLogIndex > lastLog.Index {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	target := rf.logs[args.PrevLogIndex]
	if target.Term != args.PrevLogTerm {
		rf.logs = rf.logs[:args.PrevLogIndex]
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else  {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
	}
	if len(args.Entries) > 0 {
		rf.logs = append(rf.logs, args.Entries...)
		rf.DPrintf("rf.logs update: %+v", rf.logs)
	}

	if args.LeaderCommit > rf.commitIndex {
		// TODO, 不能直接apply
		minIdx := min(args.LeaderCommit, len(rf.logs)-1)
		for i := rf.commitIndex + 1; i <= minIdx; i++ {
			rf.updateStateMachine(ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[i].Command,
				CommandIndex: i,
			})
		}
		rf.commitIndex = minIdx
	}
	// }

	reply.Term = rf.currentTerm
	reply.Success = true
	return
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
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
			prevLog := rf.logs[len(rf.logs)-1]
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
			}
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, args, &reply)
			if !ok {
				return
			}

			if reply.Term > rf.CurrentTerm() {
				rf.toFollowerCh <- toFollowerEvent{
					term:   reply.Term,
					server: rf.me,
				}
				return
			}

		}(idx)
	}
}
