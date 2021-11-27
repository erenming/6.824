package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type TaskState uint8

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type ComputePhase uint8

const (
	PhaseMap ComputePhase = iota
	PhaseReduce
	PhaseDone
)

// Your definitions here.
type Master struct {
	mapTasks map[int]*Task
	mapMutex sync.Mutex

	reduceTasks map[int]*Task
	reduceMutex sync.Mutex

	updateTask chan Task
	readyTask  chan Task

	workerPool   map[string]*workerInfo
	workerMutex  sync.Mutex
	workerExpire time.Duration

	phase atomic.Value

	nReduce int
	done    chan struct{}
}

type workerInfo struct {
	ID       string
	lastSeen time.Time
	timer    *time.Timer
	task     Task
}

type Task struct {
	ID        int
	Type      TaskType
	Filenames []string
	State     TaskState
	// update action
	appendFile bool
}

func (m *Master) existedWorker(id string) bool {
	m.workerMutex.Lock()
	defer m.workerMutex.Unlock()
	_, ok := m.workerPool[id]
	return ok
}

func (m *Master) distributeMapTask(reply *TaskResponse, task Task) error {
	reply.Task = MapTask
	reply.NumReduce = m.nReduce
	reply.Filenames = task.Filenames
	reply.ID = task.ID

	// log.Printf("distributeMapTask %d", reply.ID)
	return nil
}

func (m *Master) distributeReduceTask(reply *TaskResponse, task Task) error {
	reply.Task = ReduceTask
	reply.Filenames = task.Filenames
	reply.ID = task.ID

	// sort.Strings(reply.Filenames)
	// log.Printf("distributeReduceTask %d, filenames: %+v", reply.ID, reply.Filenames)
	return nil
}

func (m *Master) selectIdleMapTask() (*Task, bool) {
	m.mapMutex.Lock()
	defer m.mapMutex.Unlock()
	for _, v := range m.mapTasks {
		if v.State == Idle {
			return v, true
		}
	}
	return nil, false
}

func (m *Master) selectIdleReduceTask() (*Task, bool) {
	m.reduceMutex.Lock()
	defer m.reduceMutex.Unlock()
	for _, v := range m.reduceTasks {
		if v.State == Idle {
			return v, true
		}
	}
	return nil, false
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
	phase := m.phase.Load().(ComputePhase)
	return phase == PhaseDone
}

func (m *Master) consumeMapTask() {
	for {
		select {
		case t := <-m.updateTask:
			switch t.Type {
			case MapTask:
				m.mapMutex.Lock()
				if t.State == Completed {
					delete(m.mapTasks, t.ID)
				} else {
					m.mapTasks[t.ID] = &t
				}
				if len(m.mapTasks) == 0 {
					m.phase.Store(PhaseReduce)
				}
				m.mapMutex.Unlock()
			case ReduceTask:
				m.reduceMutex.Lock()
				if t.State == Completed {
					delete(m.reduceTasks, t.ID)
				} else {
					val, ok := m.reduceTasks[t.ID]
					if ok && t.appendFile {
						t.Filenames = append(t.Filenames, val.Filenames...)
					}
					m.reduceTasks[t.ID] = &t
				}

				if len(m.reduceTasks) == 0 {
					m.phase.Store(PhaseDone)
				}
				m.reduceMutex.Unlock()
			default:
				log.Printf("unknown task type: %d", t.Type)
			}
		case <-m.done:
			return
		}
	}
}

func (m *Master) schedule() {
	for {
		switch m.phase.Load().(ComputePhase) {
		case PhaseMap:
			t, ok := m.selectIdleMapTask()
			if ok {
				t.State = InProgress
				m.readyTask <- *t
			}
		case PhaseReduce:
			t, ok := m.selectIdleReduceTask()
			if ok {
				t.State = InProgress
				m.readyTask <- *t
			}
		default:
			return
		}
		time.Sleep(time.Second)
	}

}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nReduce:      nReduce,
		workerPool:   map[string]*workerInfo{},
		reduceTasks:  map[int]*Task{},
		workerExpire: 10 * time.Second,
		done:         make(chan struct{}),
		updateTask:   make(chan Task),
		readyTask:    make(chan Task),
	}

	mapTasks := make(map[int]*Task, 0)
	idx := 0
	for _, item := range files {
		ms, err := filepath.Glob(item)
		if err != nil {
			panic(err)
		}
		for _, fn := range ms {
			mapTasks[idx] = &Task{
				ID:        idx,
				Filenames: []string{fn},
				State:     Idle,
			}
			idx++
		}
	}
	m.mapTasks = mapTasks

	m.phase.Store(PhaseMap)

	m.server()

	go m.consumeMapTask()
	go m.schedule()
	return &m
}
