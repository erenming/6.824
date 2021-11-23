package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type TaskType uint8

const (
	MapTask TaskType = iota
	ReduceTask
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskRequest struct {
}

type TaskResponse struct {
	Task      TaskType
	ID        int
	Filenames []string

	NumReduce  int
	NoMoreTask bool
}

type MapTaskReport struct {
	ID          int
	ReduceInfos []*ReduceInfo
}

type ReduceInfo struct {
	InterFileLocation string
	ReduceTaskID      int
}

type MapTaskReportResponse struct {
}

type ReduceTaskReport struct {
	ReduceTaskID int
}

type ReduceTaskReportResponse struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
