package raft

import (
	// "sync"
	"time"

	sync "github.com/sasha-s/go-deadlock"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term                      int
	CandidateID               int
	LastLogTerm, LastLogIndex int
	TraceID                   string
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if args.Term > rf.CurrentTerm() {
		rf.convertToFollower(toFollowerEvent{
			term:    args.Term,
			server:  args.CandidateID,
			traceID: args.TraceID,
		})
		rf.persist()
	}

	if !rf.isMoreUpToDate(args) {
		rf.DPrintf("[%s]isMoreUpToDate rejected, <%d>", args.TraceID, rf.currentTerm)
		reply.Term = rf.CurrentTerm()
		reply.VoteGranted = false
		return
	}

	if args.Term < rf.CurrentTerm() {
		rf.DPrintf("[%s]args.Term < rf.currentTerm rejected, <%d>", args.TraceID, rf.CurrentTerm())
		reply.Term = rf.CurrentTerm()
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		rf.DPrintf("[%s]voted success", args.TraceID)
		rf.mu.Lock()
		rf.votedFor = args.CandidateID
		rf.electionTimeout = randomElectionTimeout()
		rf.refreshTime = time.Now()
		rf.mu.Unlock()
		rf.persist()

		reply.Term = rf.CurrentTerm()
		reply.VoteGranted = true
	} else {
		rf.DPrintf("[%s]end rejected, <%d>", args.TraceID, rf.CurrentTerm())
		reply.Term = rf.CurrentTerm()
		reply.VoteGranted = false
	}
}

// check args is more update-to-date server
func (rf *Raft) isMoreUpToDate(args *RequestVoteArgs) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	prevLog := rf.logs[len(rf.logs)-1]
	if args.LastLogTerm == prevLog.Term {
		return args.LastLogIndex >= prevLog.Index
	} else {
		return args.LastLogTerm >= prevLog.Term
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) runElection() {
	tranceID := RandStringBytes()
	rf.SetRole(CANDIDATE)
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.mu.Unlock()

	rf.persist()

	rf.mu.RLock()
	lastLog := rf.logs[len(rf.logs)-1]
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogTerm:  lastLog.Term,
		LastLogIndex: lastLog.Index,
		TraceID:      tranceID,
	}
	rf.mu.RUnlock()
	rf.DPrintf("[%s]request vote, <%d>", args.TraceID, args.Term)

	ch := rf.broadcastRV(args)
	cnt, n := 0, len(rf.peers)
	for {
		select {
		case <-rf.doneServer:
			return
		case <-time.After(rf.ElectionTimeout()):
			rf.mu.Lock()
			rf.votedFor = -1
			rf.mu.Unlock()
			return
		case reply, ok := <-ch:
			if !ok {
				return
			}
			if args.Term < reply.Term {
				if rf.CurrentTerm() < reply.Term {
					rf.convertToFollower(toFollowerEvent{
						term:    reply.Term,
						server:  rf.me,
						traceID: args.TraceID,
					})
				}
				return
			}

			if reply.VoteGranted {
				rf.DPrintf("[%s]vote received, term: %d", args.TraceID, reply.Term)
				cnt++
			}

			if isMajority(cnt, n) && rf.Role() == CANDIDATE {
				rf.toLeaderCh <- struct{}{}
				return
			}
		}
	}

}

func (rf *Raft) broadcastRV(args *RequestVoteArgs) chan RequestVoteReply {
	ch := make(chan RequestVoteReply)
	var wg sync.WaitGroup
	wg.Add(len(rf.peers) - 1)
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(server int) {
			defer wg.Done()
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, &reply)
			if !ok {
				return
			}
			select {
			case ch <- reply:
			default:
			}
		}(idx)

	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}
