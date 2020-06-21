package mr

import (
	"fmt"
	"log"
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
	Ready     = iota
	Running   = iota
)

type Task struct {
	TaskId           int
	Filenames        []string
	TaskState        int
	TaskType         int
	TaskIndex        int
	AssignedWorkerId int
}

type Master struct {
	// Your definitions here.
	idMutuex sync.Mutex
	mu       sync.Mutex

	completedReduceTasks int
	nReduce              int
	nMap                 int

	workerIdGen      int // Generate Ids for workers
	workersAvailable map[int]bool
	workersStats     map[int]int

	taskIdGen    int
	taskIdMapper map[int]*Task

	intermediateFiles map[int][]string

	outputFiles []string
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

func (m *Master) GenTaskId() int {
	m.idMutuex.Lock()
	defer m.idMutuex.Unlock()

	retId := m.taskIdGen
	m.taskIdGen++

	return retId
}

func (m *Master) GenWorkerId() int {
	m.idMutuex.Lock()
	defer m.idMutuex.Unlock()

	retId := m.workerIdGen
	m.workerIdGen++

	return retId
}

// Called by worker when started to get an ID
func (m *Master) RegisterWorker(empty string, reply *WorkerIdReply) error {
	workerId := m.GenWorkerId()
	reply.Id = workerId
	m.mu.Lock()
	m.workersAvailable[workerId] = true
	m.mu.Unlock()
	return nil
}

// Called by worker to get a task to work on (Map or Reduce)
func (m *Master) GetTask(request *GetTaskRequest, reply *GetTaskReply) error {
	var retTask *Task
	taskFound := false
	workerId := request.WorkerId

	m.mu.Lock()
	fmt.Println("MapLength:", len(m.taskIdMapper))
	for _, task := range m.taskIdMapper {
		if task.TaskState == Ready {
			retTask = task
			(*task).TaskState = Running
			(*task).AssignedWorkerId = workerId
			taskFound = true
			break
		}
	}
	m.mu.Unlock()

	if !taskFound {
		reply.TaskType = None
		return nil
	}
	fmt.Println("Returning a type", reply.TaskType, "task")

	reply.TaskType = retTask.TaskType
	reply.Filenames = retTask.Filenames
	reply.TaskId = retTask.TaskId
	reply.TaskIndex = retTask.TaskIndex
	reply.NumReduce = m.nReduce
	return nil
}

func (m *Master) WorkerTaskCompleted(request *TaskCompletedRequest, reply *TaskCompletedReply) error {
	taskId := request.TaskId
	taskType := request.TaskType
	workerId := request.WorkerId
	//reduceOutputFile := request.ReduceOutputFile
	//filenamesMapper := request.FilenamesMapper
	fmt.Println("Task with Id:", taskId, "Completed by worker with Id:", workerId)

	if taskType == MapTask {
		mapCompleted := false
		m.mu.Lock()
		task := m.taskIdMapper[taskId]

		// Handling if worker detected as dead then reply received from it
		if task.TaskState == Running {
			task.TaskState = Completed

			for k, v := range request.FilenamesMapper {
				m.intermediateFiles[k] = append(m.intermediateFiles[k], v)
			}
			m.nMap--
		}

		if m.nMap == 0 {
			mapCompleted = true
		}

		m.mu.Unlock()

		if mapCompleted {
			m.GenerateReduceTasks()
		}

	} else if taskType == ReduceTask {
		m.mu.Lock()
		task := m.taskIdMapper[taskId]

		// Handling if worker detected as dead when reply received from it
		if task.TaskState == Ready {
			task.TaskState = Completed
			m.outputFiles = append(m.outputFiles, request.ReduceOutputFile)
			m.completedReduceTasks++
		}

		m.mu.Unlock()
	}

	return nil
}

// Create a map task for each file
func (m *Master) GenerateMapTasks(filenames []string) {
	m.nMap = len(filenames)
	for _, filename := range filenames {
		task := Task{}
		task.TaskId = m.GenTaskId()
		task.Filenames = []string{filename}
		task.TaskType = MapTask
		task.TaskState = Ready
		task.AssignedWorkerId = -1

		m.taskIdMapper[task.TaskId] = &task
	}
}

func (m *Master) GenerateReduceTasks() {
	fmt.Println("Generating Reduce Tasks ..")

	for k, v := range m.intermediateFiles {
		task := Task{}
		task.TaskId = m.GenTaskId()
		task.Filenames = v
		task.TaskType = ReduceTask
		task.TaskState = Ready
		task.AssignedWorkerId = -1
		task.TaskIndex = k

		m.mu.Lock()
		m.taskIdMapper[task.TaskId] = &task

		m.mu.Unlock()
	}

	fmt.Println("Completed reduce tasks generation")
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
	m.intermediateFiles = make(map[int][]string)
	m.taskIdMapper = make(map[int]*Task)
	m.taskIdGen = 1
	m.completedReduceTasks = 0

	m.GenerateMapTasks(files)
	m.server()
	return &m
}
