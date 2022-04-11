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
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = -1
	reply.TraceID = args.TraceID

	if term := rf.CurrentTerm(); args.Term < term {
		reply.Term = term
		reply.Success = false
		return
	}
	defer rf.persist()

	if rf.Role() != FOLLOWER {
		rf.convertToFollower(toFollowerEvent{
			term:    args.Term,
			server:  rf.me,
			traceID: args.TraceID,
		})
		return
	}

	rf.mu.Lock()
	rf.currentTerm = args.Term
	rf.refreshTime = time.Now()
	rf.mu.Unlock()

	rf.mu.RLock()
	lastLog := rf.logs[len(rf.logs)-1]
	if args.PrevLogIndex > lastLog.Index {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XLen = len(rf.logs)
		rf.mu.RUnlock()
		rf.DPrintf("[%s]1-reply: %+v", args.TraceID, reply)
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
		rf.mu.RUnlock()
		rf.DPrintf("[%s]2-reply: %+v", args.TraceID, reply)
		return
	}

	// 检查并忽略之前的logReplica
	if len(args.Entries) > 0 && isOldLogReplica(rf.logs, args.Entries) {
		reply.Term = rf.currentTerm
		reply.Success = true
		// rf.DPrintf("ignore old rpc.")
		rf.mu.RUnlock()
		return
	}
	rf.mu.RUnlock()

	if len(args.Entries) > 0 {
		// clean invalid log entries
		rf.mu.Lock()
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
		rf.mu.Unlock()
		// rf.persist()
	}

	rf.mu.Lock()
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
	rf.mu.Unlock()

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
			traceID := RandStringBytes()
			if rf.killed() || rf.Role() != LEADER {
				return
			}
			rf.mu.RLock()
			tmp := rf.logs[rf.nextIndex[server]:]
			logs := make([]LogEntry, len(tmp))
			for i := 0; i < len(tmp); i++ {
				logs[i] = tmp[i]
			}
			if rf.nextIndex[server]-1 == -1 {
				rf.DPrintf("[%s] index out of range [-1], term: %d, retry: %+v, role: %s, nindex: %+v", traceID, rf.currentTerm, retry, rf.Role(), rf.nextIndex)
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
			rf.mu.RUnlock()

		redo:
			reply, ok := rf.reqAppendRPC(server, args)
			if !ok {
				if retry {
					goto redo
				} else {
					return
				}
			}

			if reply.Term > args.Term {
				if reply.Term > rf.CurrentTerm() {
					rf.convertToFollower(toFollowerEvent{
						term:    reply.Term,
						server:  rf.me,
						traceID: args.TraceID,
					})
				}
				return
			}

			if !reply.Success {
				// handle rejected AppendRPC
				rf.mu.Lock()
				if retry && rf.Role() != LEADER {
					rf.mu.Unlock()
					return
				}
				if reply.XIndex != -1 {
					rf.nextIndex[server] = reply.XIndex
					rf.DPrintf("[%s]reply.XIndex from %d, nindx: %+v, retry: %+v, reply: %+v", args.TraceID, server, rf.nextIndex, retry, reply)
				} else if reply.XTerm != -1 {
					j, term := args.PrevLogIndex, args.PrevLogTerm
					for ; j > 0; j-- {
						if rf.logs[j].Term != term {
							break
						}
					}
					rf.nextIndex[server] = j + 1
					rf.DPrintf("[%s]reply.XTerm from %d, nindx: %+v", args.TraceID, server, rf.nextIndex)
					// back xterm
				} else if reply.XLen != -1 {
					rf.nextIndex[server] = reply.XLen
					rf.DPrintf("[%s]reply.XLen from %d, nindx: %+v", args.TraceID, server, rf.nextIndex)
				} else {
					// rf.nextIndex[server]--
					// rf.DPrintf("[%s]decrease nindx from %d, nindx: %+v", args.TraceID, server, rf.nextIndex)
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
			if retry && rf.Role() != LEADER {
				rf.mu.Unlock()
				return
			}
			if rf.nextIndex[server] == 0 {
				rf.DPrintf("[%s] term: %d, logs: %+v, retry: %+v, role: %s, nindex", args.TraceID, rf.currentTerm, betterLogs(rf.logs), retry, rf.Role(), rf.nextIndex)
			}
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
