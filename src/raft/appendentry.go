package raft

import (
	"sync/atomic"
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

		reply.XLen = len(rf.logs)

		return
	}

	target := rf.logs[args.PrevLogIndex]
	if args.PrevLogIndex != target.Index || args.PrevLogTerm != target.Term {
		reply.Term = rf.currentTerm
		reply.Success = false

		j := target.Index
		for ; j > 0; j-- {
			if rf.logs[j].Term != target.Term {
				break
			}
		}
		reply.XTerm = target.Term
		if j > 0 {
			reply.XIndex = j + 1
		}
		return
	}

	// 检查并忽略之前的logReplica
	if len(args.Entries) > 0 && isOldLogReplica(rf.logs, args.Entries) {
		reply.Term = rf.currentTerm
		reply.Success = true
		// rf.DPrintf("ignore old rpc.")
		return
	}

	if len(args.Entries) > 0 {
		// clean invalid log entries
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
	}

	if args.LeaderCommit > rf.commitIndex {
		minIdx := min(args.LeaderCommit, len(rf.logs)-1)
		// rf.DPrintf("[%s]commit indx diff, <%d, %d>, term: %d", args.TraceID, rf.commitIndex, minIdx, rf.currentTerm)
		i := rf.commitIndex + 1
		for ; i <= minIdx; i++ {
			cur := rf.logs[i]
			if args.PrevLogTerm < cur.Term {
				break
			}
			rf.updateStateMachine(ApplyMsg{
				CommandValid: true,
				Command:      cur.Command,
				CommandIndex: i,
			})
		}
		rf.commitIndex = i - 1
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

func (rf *Raft) broadcastAppendRPC(retry bool) {
	cnt := uint64(0)
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}

		go func(server int) {
		redo:
			if rf.killed() || rf.Role() != LEADER {
				return
			}
			traceID := RandStringBytes()
			rf.mu.Lock()

			tmp := rf.logs[rf.nextIndex[server]:]
			logs := make([]LogEntry, len(tmp))
			for i := 0; i < len(tmp); i++ {
				logs[i] = tmp[i]
			}

			prevLog := rf.logs[rf.nextIndex[server]-1]
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				Entries:      logs,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
				TraceID:      traceID,
			}
			if retry {
				// rf.DPrintf("[%s]replica to %d, <%d, %d>, entries: %+v", args.TraceID, server, args.PrevLogTerm, args.PrevLogIndex, betterLogs(args.Entries))
			}
			rf.mu.Unlock()
			reply, ok := rf.reqAppendRPC(server, args)
			if !ok {
				if retry {
					goto redo
				} else {
					return
				}
			}

			if reply.Term > rf.CurrentTerm() {
				rf.toFollowerCh <- toFollowerEvent{
					term:    reply.Term,
					server:  rf.me,
					traceID: args.TraceID,
				}
				return
			}

			if !reply.Success {
				// handle rejected AppendRPC
				rf.mu.Lock()
				preidx := rf.nextIndex[server]
				if reply.XIndex != -1 {
					rf.nextIndex[server] = reply.XIndex
				} else if reply.XTerm != -1 {
					j, term := args.PrevLogIndex, args.PrevLogTerm
					for ; j > 0; j-- {
						if rf.logs[j].Term != term {
							break
						}
					}
					rf.nextIndex[server] = j + 1
					// back xterm
				} else if reply.XLen != -1 {
					rf.nextIndex[server] = reply.XLen
				} else {
					rf.nextIndex[server]--
				}
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				rf.mu.Unlock()
				if retry {
					goto redo
				} else {
					return
				}
			}

			// handle nextIndex&matchIndex
			rf.mu.Lock()
			curPrefLog := rf.logs[rf.nextIndex[server]-1]
			if curPrefLog.Index != args.PrevLogIndex || curPrefLog.Term != args.PrevLogTerm {
				// old appendRPC reply received, then ignore
				rf.mu.Unlock()
				return
			}
			rf.nextIndex[server] += len(logs)
			rf.matchIndex[server] += len(logs)
			rf.mu.Unlock()

			atomic.AddUint64(&cnt, 1)
			if !isMajority(int(atomic.LoadUint64(&cnt)), len(rf.peers)) {
				return
			}

			rf.checkAndCommit()
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
	toCompare := logs[entries[0].Index:]
	i := 0
	for ; i < len(toCompare) && i < len(entries); i++ {
		src, dst := entries[i], toCompare[i]
		if src.Term > dst.Term {
			return false
		} else if src.Term < dst.Term {
			return true
		} else {
			if src.Index == dst.Index {
				continue
			} else {
				return false
			}
		}
	}
	if i == len(entries) {
		return true
	}
	return false
}
