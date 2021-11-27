package mr

import (
	"log"
	"time"
)

// Your code here -- RPC handlers for the worker to call.
func (m *Master) HeartBeat(args *HeartBeatReq, reply *HeartBeatResp) error {
	if m.phase.Load().(ComputePhase) >= PhaseDone {
		return nil
	}
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
	if m.phase.Load().(ComputePhase) >= PhaseDone {
		return nil
	}
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
		// reset
		m.updateTask <- Task{
			ID:        w.task.ID,
			Type:      w.task.Type,
			Filenames: w.task.Filenames,
			State:     Idle,
		}
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
		log.Printf("master done")
	}
	return nil
}

func (m *Master) ReportMapResult(args *MapTaskReport, reply *MapTaskReportResponse) error {
	if !m.existedWorker(args.WorkerID) {
		return nil
	}

	for _, info := range args.ReduceInfos {
		m.updateTask <- Task{
			Type:       ReduceTask,
			ID:         info.ReduceTaskID,
			Filenames:  []string{info.InterFileLocation},
			State:      Idle,
			appendFile: true,
		}
	}

	m.updateTask <- Task{
		Type:  MapTask,
		ID:    args.MapTaskID,
		State: Completed,
	}

	return nil
}

func (m *Master) ReportReduceResult(args *ReduceTaskReport, reply *ReduceTaskReportResponse) error {
	if !m.existedWorker(args.WorkerID) {
		return nil
	}

	m.updateTask <- Task{
		Type:  ReduceTask,
		ID:    args.ReduceTaskID,
		State: Completed,
	}
	return nil
}

func (m *Master) TaskDistribute(args *TaskRequest, reply *TaskResponse) error {
	if !m.existedWorker(args.WorkerID) {
		return nil
	}

	phase := m.phase.Load().(ComputePhase)
	switch phase {
	case PhaseMap, PhaseReduce:
		for t := range m.readyTask {
			m.workerMutex.Lock()
			m.workerPool[args.WorkerID].task = t
			m.workerMutex.Unlock()

			switch t.Type {
			case MapTask:
				reply.Task = MapTask
				return m.distributeMapTask(reply, t)
			case ReduceTask:
				reply.Task = ReduceTask
				return m.distributeReduceTask(reply, t)
			}
		}
	case PhaseDone:
		reply.NoMoreTask = true
	}
	return nil
}
