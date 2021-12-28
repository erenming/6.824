package raft

import (
	"context"
	"log"
)

type event string

const (
	// follower to candidate
	F2C event = "F2C"
	// candidate to follower
	C2F event = "C2F"
	// candidate to leader
	C2L event = "C2L"
)

func (rf *Raft) eventLoop(ctx context.Context) {
	for {
		select {
		case e := <-rf.eventCh:
			rf.handleEvent(e)
		case <-ctx.Done():
		}
	}
}

func (rf *Raft) handleEvent(e event) {
	switch e {
	case F2C:
		log.Printf("F2C start, me: %d\n", rf.me)
		rf.mu.Lock()
		if rf.state != follower {
			return
		}
		rf.currentTerm++
		rf.state = candidate
		rf.mu.Unlock()
		rf.startElection()
	case C2L:
		log.Println("C2L start")
		rf.mu.Lock()
		if rf.state != candidate {
			return
		}
		rf.state = leader
		rf.mu.Unlock()

		rf.doneHB = make(chan struct{})
		go rf.runHeartbeat(rf.doneHB)
	case C2F:
		rf.mu.Lock()
		if rf.state != candidate {
			return
		}
		rf.state = follower
		rf.mu.Unlock()
	}
}
