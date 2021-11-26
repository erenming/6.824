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
	PhaseReduceDone
	PhaseDone
)

// Your definitions here.
type Master struct {
	mapTasks map[int]*Task
	mapMutex sync.Mutex

	reduceTasks map[int]*Task
	reduceMutex sync.Mutex

	workerPool   map[string]*workerInfo
	workerMutex  sync.Mutex
	workerExpire time.Duration

	phase atomic.Value

	nReduce int
}

type workerInfo struct {
	ID       string
	lastSeen time.Time
	timer    *time.Timer
}

type Task struct {
	ID        int
	Filenames []string
	State     TaskState
}

type HeartBeatResp struct {
}

type HeartBeatReq struct {
	WorkerID string
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) HeartBeat(args *HeartBeatReq, reply *HeartBeatResp) error {
	m.workerMutex.Lock()
	defer m.workerMutex.Unlock()
	w, ok := m.workerPool[args.WorkerID]
	if !ok {
		m.addWorker(args.WorkerID)
		return nil
	}

	// update worker
	w.lastSeen = time.Now()
	if !w.timer.Stop() {
		select {
		case <-w.timer.C:
		default:
		}
	}
	w.timer.Reset(m.workerExpire)
	return nil
}

func (m *Master) WorkerRegister(args *WorkerRegisterReq, reply *WorkerRegisterResp) error {
	m.addWorker(args.WorkerID)
	return nil
}

func (m *Master) addWorker(id string) {
	w := &workerInfo{
		ID:       id,
		lastSeen: time.Now(),
	}
	timer := time.AfterFunc(m.workerExpire, func() {
		m.workerMutex.Lock()
		delete(m.workerPool, w.ID)
		m.workerMutex.Unlock()
	})
	w.timer = timer

	m.workerMutex.Lock()
	m.workerPool[w.ID] = w
	m.workerMutex.Unlock()
}

func (m *Master) WorkerLogout(args *WorkerLogoutReq, reply *WorkerLogOutResp) error {
	m.workerMutex.Lock()
	defer m.workerMutex.Unlock()
	delete(m.workerPool, args.WorkerID)
	if len(m.workerPool) == 0 {
		m.phase.Store(PhaseDone)
	}
	return nil
}

func (m *Master) ReportMapResult(args *MapTaskReport, reply *MapTaskReportResponse) error {
	if !m.existedWorker(args.WorkerID) {
		return nil
	}

	m.reduceMutex.Lock()
	for _, info := range args.ReduceInfos {
		t, ok := m.reduceTasks[info.ReduceTaskID]
		if !ok {
			m.reduceTasks[info.ReduceTaskID] = &Task{
				ID:        info.ReduceTaskID,
				Filenames: []string{info.InterFileLocation},
				State:     Idle,
			}
		} else {
			t.Filenames = append(t.Filenames, info.InterFileLocation)
		}
	}
	m.reduceMutex.Unlock()

	m.mapMutex.Lock()
	delete(m.mapTasks, args.MapTaskID)
	if len(m.mapTasks) == 0 {
		m.phase.Store(PhaseReduce)
	}
	m.mapMutex.Unlock()

	return nil
}

func (m *Master) ReportReduceResult(args *ReduceTaskReport, reply *ReduceTaskReportResponse) error {
	if !m.existedWorker(args.WorkerID) {
		return nil
	}

	m.reduceMutex.Lock()
	delete(m.reduceTasks, args.ReduceTaskID)
	if len(m.reduceTasks) == 0 {
		m.phase.Store(PhaseReduceDone)
	}
	m.reduceMutex.Unlock()
	return nil
}

func (m *Master) TaskDistribute(args *TaskRequest, reply *TaskResponse) error {
	if !m.existedWorker(args.WorkerID) {
		return nil
	}

	phase := m.phase.Load().(ComputePhase)
	switch phase {
	case PhaseMap:
		reply.Task = MapTask
		t, ok := m.selectIdleMapTask()
		if !ok {
			return nil
		}
		return m.distributeMapTask(reply, t)
	case PhaseReduce:
		reply.Task = ReduceTask
		t, ok := m.selectIdleReduceTask()
		if !ok {
			return nil
		}
		return m.distributeReduceTask(reply, t)
	case PhaseReduceDone, PhaseDone:
		reply.NoMoreTask = true
	}
	return nil
}

func (m *Master) existedWorker(id string) bool {
	m.workerMutex.Lock()
	defer m.workerMutex.Unlock()
	_, ok := m.workerPool[id]
	return ok
}

func (m *Master) distributeMapTask(reply *TaskResponse, task *Task) error {
	m.mapMutex.Lock()
	defer m.mapMutex.Unlock()

	reply.Task = MapTask
	reply.NumReduce = m.nReduce
	reply.Filenames = task.Filenames
	reply.ID = task.ID

	task.State = InProgress
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

	sort.Strings(reply.Filenames)
	log.Printf("distributeReduceTask %d, filenames: %+v", reply.ID, reply.Filenames)
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
	return m.phase.Load().(ComputePhase) == PhaseDone
}

func (m *Master) completedMap() bool {
	m.mapMutex.Lock()
	defer m.mapMutex.Unlock()
	return len(m.mapTasks) == 0
}

func (m *Master) completedReduce() bool {
	return len(m.reduceTasks) == 0
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
	return &m
}
