package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 0

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func randDuration(start, end time.Duration) time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Intn(int(end)-int(start)) + int(start))
}

func randomElectionTimeout() time.Duration {
	return randDuration(300*time.Millisecond, 500*time.Millisecond)
}