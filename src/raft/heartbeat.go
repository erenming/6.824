package raft

import (
	"log"
	"sync"
	"time"
)

func (rf *Raft) runHeartbeat(done chan struct{}) {
	log.Printf("heatbeat loop started!")
	for {
		select {
		case <-done:
			return
		default:
		}

		_ = rf.parallelismAppendEntities(&AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		})

		time.Sleep(rf.electionTimout / 3)
	}
}

func (rf *Raft) parallelismAppendEntities(args *AppendEntriesArgs) []AppendEntriesReply {
	replies := make([]AppendEntriesReply, len(rf.peers))
	var wg sync.WaitGroup
	wg.Add(len(rf.peers))
	for idx, _ := range rf.peers {
		go func(sid int) {
			defer wg.Done()
			ok := rf.sendAppendEntities(sid, args, &replies[sid])

			if !ok {
				log.Println("sendAppendEntities failed")
			}

		}(idx)
	}
	wg.Wait()
	return replies
}
