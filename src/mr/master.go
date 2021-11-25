package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type TaskState uint8

const (
	Idle = iota
	InProgress
	Completed
)

// Your definitions here.
type Master struct {
	mapTasks []*Task
	mapMutex sync.Mutex

	reduceTasks []*Task
	reduceMutex sync.Mutex
	reduceChan  chan *Task

	done    chan struct{}
	nReduce int
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
		if info.ReduceTaskID == 0 {
			log.Printf("map task id: %d, file: %s", args.ID, info.InterFileLocation)
		}
		t := m.reduceTasks[info.ReduceTaskID]
		t.State = Idle
		t.Filenames = append(t.Filenames, info.InterFileLocation)
	}
	m.reduceMutex.Unlock()

	m.mapMutex.Lock()
	m.mapTasks[args.ID].State = Completed
	m.mapMutex.Unlock()

	return nil
}

func (m *Master) ReportReduceResult(args *ReduceTaskReport, reply *ReduceTaskReportResponse) error {
	m.reduceMutex.Lock()
	task := m.reduceTasks[args.ReduceTaskID]
	if len(task.Filenames) == 0 {
		task.State = Completed
	} else {
		log.Printf("counldn't complete task %d filenames: %+v", task.ID, task.Filenames)
	}
	m.reduceMutex.Unlock()
	return nil
}

func (m *Master) TaskDistribute(args *TaskRequest, reply *TaskResponse) error {
	for !m.completedMap() {
		t, ok := m.selectIdleMapTask()
		if !ok {
			time.Sleep(time.Second)
			continue
		}
		return m.distributeMapTask(reply, t)
	}

	for !m.completedReduce() {
		t, ok := m.selectIdleReduceTask()
		if !ok {
			time.Sleep(time.Second)
			continue
		}
		return m.distributeReduceTask(reply, t)
	}

	reply.NoMoreTask = true
	return nil
}

func (m *Master) distributeMapTask(reply *TaskResponse, task *Task) error {
	m.mapMutex.Lock()
	defer m.mapMutex.Unlock()

	reply.Task = MapTask
	reply.NumReduce = m.nReduce
	reply.Filenames = task.Filenames
	reply.ID = task.ID

	task.State = InProgress
	task.Filenames = []string{}
	log.Printf("distributeMapTask %d", reply.ID)
	return nil
}

func (m *Master) distributeReduceTask(reply *TaskResponse, task *Task) error {
	m.reduceMutex.Lock()
	defer m.reduceMutex.Unlock()

	reply.Task = ReduceTask
	reply.Filenames = task.Filenames
	reply.ID = task.ID

	task.State = InProgress
	task.Filenames = []string{}

	sort.Strings(reply.Filenames)
	log.Printf("distributeReduceTask %d, filenames: %+v", reply.ID, reply.Filenames)
	return nil
}

func (m *Master) selectIdleMapTask() (*Task, bool) {
	m.mapMutex.Lock()
	defer m.mapMutex.Unlock()
	idx := -1
	for i, item := range m.mapTasks {
		if item.State == Idle {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil, false
	}
	return m.mapTasks[idx], true
}

func (m *Master) selectIdleReduceTask() (*Task, bool) {
	m.reduceMutex.Lock()
	defer m.reduceMutex.Unlock()
	idx := -1
	for i, item := range m.reduceTasks {
		if item.State == Idle {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil, false
	}
	return m.reduceTasks[idx], true
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
	m.mapMutex.Lock()
	defer m.mapMutex.Unlock()
	for _, t := range m.mapTasks {
		if t.State != Completed {
			return false
		}
	}
	return true
}

func (m *Master) completedReduce() bool {
	m.reduceMutex.Lock()
	defer m.reduceMutex.Unlock()
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
