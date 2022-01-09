package raft

import (
	"sync"
	"time"
)

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// rpc handler for AppendEntries
// reset election timeout, avoid of elect as leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	currentTerm := rf.CurrentTerm()
	if currentTerm > args.Term {
		rf.DPrintf("invalid AppendEntries: %+v, myterm: %d", args, currentTerm)
		reply.Term = currentTerm
		reply.Success = false
		return
	}
	rf.asFollowerEvent <- followerEvent{Term: args.Term}
	rf.DPrintf("as follower event end")
	reply.Term = currentTerm
	reply.Success = true
}

func (rf *Raft) runHeartbeat() {
	ticker := time.NewTicker(time.Millisecond * 10)
	defer func() {
		rf.DPrintf("heartbeat stoped!")
		ticker.Stop()
	}()
	for {
		rf.DPrintf("send heartbeat")
		rf.broadcastAppendEntry()
		select {
		case <-rf.doneHeartBeat:
			return
		case <-ticker.C:
		}
	}
}

func (rf *Raft) broadcastAppendEntry() {
	args := &AppendEntriesArgs{
		Term:     rf.CurrentTerm(),
		LeaderId: rf.me,
	}
	ch := make(chan AppendEntriesReply, 0)
	replies := make([]AppendEntriesReply, 0)
	var wg sync.WaitGroup
	wg.Add(len(rf.peers) - 1)
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(peer int) {
			defer wg.Done()
			r := AppendEntriesReply{}
			ok := rf.sendAppendEntities(peer, args, &r)
			if ok {
				ch <- r
				rf.DPrintf("send reply: %+v, args: %+v", r, args)
			} else {
				rf.DPrintf("not ok, args: %+v", args)
			}
		}(idx)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	for r := range ch {
		replies = append(replies, r)
	}
	rf.handleAppendEntryReplies(replies)
}

func (rf *Raft) handleAppendEntryReplies(replies []AppendEntriesReply) {
	rf.DPrintf("handleAppendEntryReplies: %+v, myterm: %d", replies, rf.CurrentTerm())
	for _, r := range replies {
		if r.Term > rf.CurrentTerm() {
			rf.DPrintf("as follower")
			rf.asFollowerEvent <- followerEvent{Term: r.Term}
			rf.DPrintf("as follower done")
		}
	}
}
