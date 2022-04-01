package raft

import (
	"fmt"
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	if args.Term < rf.CurrentTerm() {
		reply.Term = rf.CurrentTerm()
		reply.Success = false
		return
	}

	if rf.Role() != FOLLOWER {
		rf.toFollowerCh <- toFollowerEvent{
			term:    args.Term,
			server:  rf.me,
			traceID: args.TraceID,
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
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	target := rf.logs[args.PrevLogIndex]
	if args.PrevLogIndex != target.Index || args.PrevLogTerm != target.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 检查并忽略之前的logReplica
	if len(args.Entries) > 0 && isOldLogReplica(rf.logs, args.Entries) {
		reply.Term = rf.currentTerm
		reply.Success = true
		rf.DPrintf("ignore old rpc. <%+v, %+v>", betterLogs(rf.logs), betterLogs(args.Entries))
		return
	}

	if len(args.Entries) > 0 {
		rf.DPrintf("[%s]rewrite, <%d, %d>, %+v, %+v", args.TraceID, args.PrevLogTerm, args.PrevLogIndex, betterLogs(rf.logs), betterLogs(args.Entries))
		// clean invalid log entries
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
		rf.DPrintf("[%s]after-rewrite, %+v", args.TraceID, betterLogs(rf.logs))
	}

	if args.LeaderCommit > rf.commitIndex {
		minIdx := min(args.LeaderCommit, len(rf.logs)-1)
		rf.DPrintf("[%s]commit indx diff, <%d, %d>", args.TraceID, rf.commitIndex, minIdx)
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
			args := AppendEntriesArgs{

				TraceID:      RandStringBytes(),
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
			}
			rf.mu.Unlock()

			reply, ok := rf.reqAppendRPC(server, args)
			if !ok {
				return
			}

			if reply.Term > rf.CurrentTerm() {
				rf.toFollowerCh <- toFollowerEvent{
					term:    reply.Term,
					server:  rf.me,
					traceID: args.TraceID,
				}
				return
			}

		}(idx)
	}
}

func (rf *Raft) reqAppendRPC(server int, args AppendEntriesArgs) (AppendEntriesReply, bool) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
		return reply, false
	}
	return reply, true
}

func isOldLogReplica(logs, entries []LogEntry) bool {
	if len(entries) == 0 {
		return true
	}
	toCompare := logs[logs[0].Index:]
	i := 0
	for ; i < len(toCompare) && i < len(entries); i++ {
		src, dst := entries[i], toCompare[i]
		if src.Equal(dst) {
			continue
		}

		if src.Term < dst.Term {
			fmt.Println(0)
			return true
		} else {
			fmt.Println(1)
			return false
		}

	}
	if i == len(toCompare) {
		fmt.Println(2)
		return false
	}
	return true
}
