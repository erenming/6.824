package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 1

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
	return randDuration(150*time.Millisecond, 700*time.Millisecond)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes() string {
	b := make([]byte, 10)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}


func betterLogs(data []LogEntry) [][]interface{} {
	res := make([][]interface{}, len(data))
	for i, item := range data {
		res[i] = []interface{}{item.Command, item.Term}
	}
	return res
}
