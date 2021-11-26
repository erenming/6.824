package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	id := RandStringRunes(20)
	call("Master.WorkerRegister", &WorkerRegisterReq{WorkerID: id}, &WorkerRegisterResp{})

	for {
		task := AskForTask(TaskRequest{WorkerID: id})
		if task.NoMoreTask {
			log.Printf("break")
			break
		}
		switch task.Task {
		case MapTask:
			if mr, err := maptask(task, mapf); err != nil {
				panic(err)
			} else {
				mr.WorkerID = id
				call("Master.ReportMapResult", mr, &MapTaskReportResponse{})
			}
		case ReduceTask:
			if err := reducetask(task, reducef); err != nil {
				panic(err)
			}
			call("Master.ReportReduceResult", &ReduceTaskReport{ReduceTaskID: task.ID, WorkerID: id}, &ReduceTaskReportResponse{})
		}
		time.Sleep(3 * time.Second)
	}

	call("Master.WorkerLogout", &WorkerLogoutReq{WorkerID: id}, &WorkerLogOutResp{})
}

func maptask(task TaskResponse, mapf func(string, string) []KeyValue) (*MapTaskReport, error) {
	intermediate := make([]KeyValue, 0)
	for _, filename := range task.Filenames {
		file, err := os.Open(filename)
		if err != nil {
			return nil, fmt.Errorf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			return nil, fmt.Errorf("cannot read %v", filename)
		}
		_ = file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	mr := &MapTaskReport{
		MapTaskID:   task.ID,
		ReduceInfos: make([]*ReduceInfo, 0),
	}

	reduceMap := make(map[int][]KeyValue)

	for i := 0; i < len(intermediate); i++ {
		reduceID := ihash(intermediate[i].Key) % task.NumReduce // partition
		if _, ok := reduceMap[reduceID]; !ok {
			reduceMap[reduceID] = make([]KeyValue, 0)
		}
		reduceMap[reduceID] = append(reduceMap[reduceID], intermediate[i])
	}

	for rid, list := range reduceMap {
		tmp, _ := ioutil.TempFile("", "*")
		enc := json.NewEncoder(tmp)
		for _, item := range list {
			err := enc.Encode(&item)
			if err != nil {
				return nil, err
			}
		}
		filename := fmt.Sprintf("mr-%d-%d", task.ID, rid)
		if err := os.Rename(tmp.Name(), filename); err != nil {
			return nil, err
		}

		mr.ReduceInfos = append(mr.ReduceInfos, &ReduceInfo{
			InterFileLocation: filename,
			ReduceTaskID:      rid,
		})
	}

	return mr, nil
}

//
func reducetask(task TaskResponse, reducef func(string, []string) string) error {
	filename := fmt.Sprintf("mr-out-%d", task.ID)

	if _, err := os.Stat(filename); err == nil {
		log.Printf("file %s already existed", filename)
	}

	ofile, err := os.Create(filename)
	if err != nil {
		return err
	}

	kva := make([]KeyValue, 0, 10)
	for _, filename := range task.Filenames {
		buf, err := ioutil.ReadFile(filename)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(bytes.NewBuffer(buf))
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				if err != io.EOF {
					log.Printf("decode %s", err)
				}
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j

	}
	return ofile.Close()
}

func AskForTask(req TaskRequest) TaskResponse {
	reply := TaskResponse{}
	call("Master.TaskDistribute", &req, &reply)
	return reply
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
