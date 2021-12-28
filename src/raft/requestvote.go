package raft

import (
	"log"
	"sync"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("args: %+v, currentTerm: %d me: %d \n", args, rf.currentTerm, rf.me)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// TODO more logic
	reply.VoteGranted = true
	reply.Term = args.Term
}

func (rf *Raft) runRequestVoteChecker() {
	log.Printf("runRequestVoteChecker started")
	for {
		if time.Since(rf.lastApplyTime) >= rf.electionTimout {
			// trigger request vote
			rf.eventCh <- F2C
		}
		time.Sleep(rf.electionTimout / 3)
	}
}

func (rf *Raft) startElection() {
	replies := rf.parallelismRequestVote(&RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	})
	log.Printf("replies: %+v\n", replies)
	// handle replies
	rf.handleReplies(replies)
}

func (rf *Raft) parallelismRequestVote(args *RequestVoteArgs) []RequestVoteReply {
	replies := make([]RequestVoteReply, len(rf.peers))
	var wg sync.WaitGroup
	wg.Add(len(rf.peers))
	for idx, _ := range rf.peers {
		go func(sid int) {
			defer wg.Done()
			ok := rf.sendRequestVote(sid, args, &replies[sid])

			if !ok {
				log.Println("sendRequestVote failed")
			}

		}(idx)
	}
	wg.Wait()
	return replies
}

func (rf *Raft) handleReplies(replies []RequestVoteReply) {
	winCnt := 0
	for _, r := range replies {
		if r.VoteGranted {
			winCnt++
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if winCnt*2 > len(rf.peers) && rf.state == candidate {
			rf.eventCh <- C2L

		return
	}
}
