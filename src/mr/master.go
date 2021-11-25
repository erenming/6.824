package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
)

type TaskState uint8

const (
	Idle = iota
	InProgress
	Completed
)

// Your definitions here.
type Master struct {
	mapTasks      []*Task
	nCompletedMap int
	mapMutex      sync.Mutex

	reduceTasks []*Task
	nReduce     int
	reduceMutex sync.Mutex
}

type Task struct {
	ID        int
	Filenames []string
	State     TaskState
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) ReportMapResult(args *MapTaskReport, reply *MapTaskReportResponse) error {
	m.reduceMutex.Lock()
	for _, info := range args.ReduceInfos {
		t := m.reduceTasks[info.ReduceTaskID]
		t.State = Idle
		t.Filenames = append(t.Filenames, info.InterFileLocation)
	}
	m.reduceMutex.Unlock()

	m.mapMutex.Lock()
	m.mapTasks[args.ID].State = Completed
	m.nCompletedMap++
	m.mapMutex.Unlock()
	return nil
}

func (m *Master) ReportReduceResult(args *ReduceTaskReport, reply *ReduceTaskReportResponse) error {
	m.reduceTasks[args.ReduceTaskID].State = Completed
	return nil
}

func (m *Master) TaskDistribute(args *TaskRequest, reply *TaskResponse) error {
	if !m.completedMap() { // map phase
		return m.distributeMapTask(reply)
	}
	if !m.completedReduce() { // reduce phase
		return m.distributeReduceTask(reply)
	}
	// no task
	reply.NoMoreTask = true
	return nil
}

func (m *Master) distributeMapTask(reply *TaskResponse) error {
	m.mapMutex.Lock()
	task, err := selectTask(m.mapTasks, Idle)
	m.mapMutex.Unlock()
	if err != nil {
		return fmt.Errorf("distributeMapTask: %w", err)
	}

	reply.Task = MapTask
	reply.NumReduce = m.nReduce
	reply.Filenames = task.Filenames
	reply.ID = task.ID

	m.mapMutex.Lock()
	task.State = InProgress
	task.Filenames = []string{}
	m.mapMutex.Unlock()
	log.Printf("distributeMapTask %d", reply.ID)
	return nil
}

func (m *Master) distributeReduceTask(reply *TaskResponse) error {
	m.reduceMutex.Lock()
	task, err := selectTask(m.reduceTasks, Idle)
	m.reduceMutex.Unlock()
	if err != nil {
		return fmt.Errorf("distributeReduceTask: %w", err)
	}

	reply.Task = ReduceTask
	reply.Filenames = task.Filenames
	reply.ID = task.ID

	m.reduceMutex.Lock()
	task.State = InProgress
	task.Filenames = []string{}
	m.reduceMutex.Unlock()

	log.Printf("distributeReduceTask %d", reply.ID)
	return nil
}

func selectTask(tlist []*Task, state TaskState) (*Task, error) {
	idx := -1
	for i, item := range tlist {
		if item.State == Idle {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil, fmt.Errorf("no idle task")
	}
	return tlist[idx], nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	_ = rpc.Register(m)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	_ = os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.completedMap() && m.completedReduce()
}

func (m *Master) completedMap() bool {
	return m.nCompletedMap == len(m.mapTasks)
	// for _, t := range m.mapTasks {
	// 	if t.State != Completed {
	// 		return false
	// 	}
	// }
	// return true
}

func (m *Master) completedReduce() bool {
	for _, t := range m.reduceTasks {
		if t.State != Completed {
			return false
		}
	}
	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nReduce: nReduce,
	}

	reduceTasks := make([]*Task, nReduce)
	for i := 0; i < nReduce; i++ {
		reduceTasks[i] = &Task{
			ID:        i,
			Filenames: make([]string, 0),
			State:     Idle,
		}
	}
	m.reduceTasks = reduceTasks

	mapTasks := make([]*Task, 0)
	idx := 0
	for _, item := range files {
		ms, err := filepath.Glob(item)
		if err != nil {
			panic(err)
		}
		for _, fn := range ms {
			mapTasks = append(mapTasks, &Task{
				ID:        idx,
				Filenames: []string{fn},
				State:     Idle,
			})
			idx++
		}
	}
	m.mapTasks = mapTasks

	m.server()
	return &m
}
