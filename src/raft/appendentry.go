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

	// debug
	TraceID string
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	TraceID string
	// Fast back up
	XTerm  int // term of conflicting entry
	XIndex int // index first entry of conflicting XTerm
	XLen   int // length of follower's log
}

func betterLogs(data []LogEntry) []interface{} {
	res := make([]interface{}, len(data))
	for i, item := range data {
		res[i] = item.Command
	}
	return res
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

	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = -1
	reply.TraceID = args.TraceID

	lastLog := rf.logs[len(rf.logs)-1]
	if args.PrevLogIndex > lastLog.Index {
		// reply.XIndex = lastLog.Index+1

		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	target := rf.logs[args.PrevLogIndex]
	if args.PrevLogIndex != target.Index || args.PrevLogTerm != target.Term {
		// reply.XTerm = target.Term
		// j := target.Index
		// for ; j >= 0; j-- {
		// 	if rf.logs[j].Term != target.Term {
		// 		break
		// 	}
		// }
		// reply.XIndex = j+1

		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// 检查并忽略之前的logReplica
	toCheck := rf.logs[args.PrevLogIndex+1:]
	if len(toCheck) > len(args.Entries) {
		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}

	// clean invalid log entries
	rf.logs = rf.logs[:args.PrevLogIndex+1]
	rf.logs = append(rf.logs, args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		// TODO, 不能直接apply
		minIdx := min(args.LeaderCommit, len(rf.logs)-1)
		// rf.DPrintf("[%s]commit indx diff, %d, %d", args.TraceID, rf.commitIndex, minIdx)
		for i := rf.commitIndex + 1; i <= minIdx; i++ {
			rf.updateStateMachine(ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[i].Command,
				CommandIndex: i,
			})
		}
		rf.commitIndex = minIdx
	}

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
			prevLog := rf.logs[rf.nextIndex[server]-1]
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
