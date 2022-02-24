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
	if rf.Role() != FOLLOWER {
		rf.toFollowerCh <- args.Term
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.currentTerm = args.Term
	rf.refreshTime = time.Now()

	if len(args.Entries) > 0 {
		// TODO. check and remove inconsistency logEntry
		lastIndex := len(rf.logs) - 1

		if args.PrevLogIndex > lastIndex {
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}

		prevLog := rf.logs[lastIndex]
		if prevLog.Term == args.PrevLogTerm && prevLog.Index == args.PrevLogIndex {
			rf.logs = append(rf.logs, args.Entries...)
		} else {
			rf.logs = rf.logs[:lastIndex]

			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}
	}

	if args.LeaderCommit > rf.commitIndex {
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
	reply.Term = args.Term
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
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, args, &reply)
			if !ok {
				return
			}

			if reply.Term > rf.CurrentTerm() {
				rf.toFollowerCh <- reply.Term
				return
			}

			// if reply.Success {
			// 	rf.mu.Lock()
			// 	rf.matchIndex[server] = rf.nextIndex[server] - 1
			// 	rf.mu.Unlock()
			// }
		}(idx)
	}
}
