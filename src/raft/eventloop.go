package raft

import (
	"time"
)

type followerEvent struct {
	Term int
}

func (rf *Raft) roleEventLoop() {
	for {
		select {
		case e := <-rf.asFollowerEvent:
			rf.mu.Lock()
			if rf.role == leader {
				close(rf.doneHeartBeat)
			}
			rf.role = follower
			rf.currentTerm = e.Term
			rf.votedFor = nil
			rf.lastApplyTime = time.Now()
			rf.mu.Unlock()
		case <-rf.asLeaderEvent:
			rf.DPrintf("receive asleaderEvent")
			rf.mu.Lock()
			rf.role = leader
			rf.mu.Unlock()
			rf.doneHeartBeat = make(chan struct{})
			rf.runHeartbeat()
		case <-rf.asCandidateEvent:
			rf.mu.Lock()
			rf.role = candidate
			rf.mu.Unlock()
			// start election
			// rf.DPrintf("receive asCandidateEvent")
			go func() {
				rf.electionEvent <- struct{}{}
			}()
			// rf.DPrintf("receive asCandidateEvent, end")
		}
	}
}

func (rf *Raft) ElectionEventLoop() {
	for {
		select {
		case <-rf.electionEvent:
		restart:
			rf.mu.Lock()
			rf.currentTerm++
			rf.role = candidate
			rf.votedFor = &rf.me
			rf.mu.Unlock()
			replies := rf.broadcastRequestVote()
			rf.handleRequestVoteReplies(replies)

			rf.SetVotedFor(nil)
			time.Sleep(randomElectionTimeout())
			if rf.Role() == candidate {
				goto restart
			}
		}
	}
}

func (rf *Raft) runRequestVoteChecker() {
	for {
		switch rf.Role() {
		case leader:
		case candidate:
		case follower:
			if time.Since(rf.LastApplyTime()) >= randomElectionTimeout() {
				rf.DPrintf("follower timeout election")
				rf.asCandidateEvent <- struct{}{}
			}
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
}
