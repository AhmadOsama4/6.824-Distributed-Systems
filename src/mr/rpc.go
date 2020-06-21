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

const (
	MapTask    = iota
	ReduceTask = iota
	None       = iota
)

// Add your RPC definitions here.
type WorkerIdReply struct {
	Id int
}

type GetTaskRequest struct {
	WorkerId int
}

type GetTaskReply struct {
	TaskType  int
	TaskId    int
	Filenames []string
	NumReduce int
}

type TaskCompletedRequest struct {
	WorkerId         int
	TaskId           int
	TaskType         int
	ReduceOutputFile string
	FilenamesMapper  map[int]string // maps a reduceIndex to its filename
}

type TaskCompletedReply struct {
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
