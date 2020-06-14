package mr

import (
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// State of the Worker
const (
	IdleWorker = iota
	BusyWorker = iota
)

// State of the Task
const (
	Completed = iota
	Waiting   = iota
	Running   = iota
)

type Task struct {
	TaskId    int
	Filenames []string
	TaskState int
	TaskType  int
}

type Master struct {
	// Your definitions here.
	mu sync.Mutex

	nReduce int

	workerIdGen      int // Generate Ids for workers
	workersAvailable map[int]bool
	workersStats     map[int]int

	taskIdGen  int
	tasksStats map[int]int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Called by worker when started to get an ID
func (m *Master) RegisterWorker(empty string, reply *WorkerIdReply) error {
	m.mu.Lock()
	reply.Id = m.workerIdGen
	m.workersAvailable[m.workerIdGen] = true
	m.workerIdGen++
	m.mu.Unlock()
	return nil
}

// Called by worker to get a task to work on (Map or Reduce)
func (m *Master) GetTask(request *GetTaskRequest, reply *GetTaskReply) error {
	if rand.Intn(50)%2 == 0 {
		reply.TaskType = MapTask
	} else {
		reply.TaskType = ReduceTask
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
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
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.workerIdGen = 1
	m.nReduce = nReduce
	m.workersAvailable = make(map[int]bool)
	m.workersStats = make(map[int]int)
	m.taskIdGen = 1
	m.tasksStats = make(map[int]int)

	m.server()
	return &m
}
