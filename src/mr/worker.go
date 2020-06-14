package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	CallExample()
	idArgs := WorkerIdReply{}
	//args := ExampleArgs{}
	call("Master.RegisterWorker", "", &idArgs)
	workerId := idArgs.Id
	fmt.Println("Worker Id:", workerId)

	for {
		request := GetTaskRequest{}
		reply := GetTaskReply{}

		call("Master.GetTask", &request, &reply)
		fmt.Printf("Received a %v task", reply.TaskType)

		if reply.TaskType == MapTask {

		} else if reply.TaskType == ReduceTask {

		}

		time.Sleep(time.Millisecond * 50)
	}

	// uncomment to send the Example RPC to the master.
	//CallExample()
}

func executeMapTask(
	workerId int,
	taskId int,
	filenames []string,
	mapf func(string, string) []KeyValue) map[int][]string {

	intermediateFilesMapper := make(map[int][]string)
	kvTotal := make([]KeyValue, 0)

	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		kvTotal = append(kvTotal, kva...)
	}

	mapper := make(map[string][]string)

	for _, item := range kvTotal {
		mapper[item.Key] = append(mapper[item.Key], item.Value)
	}

	for k, valueList := range mapper {
		rTask := ihash(k)
		filename := fmt.Sprintf("mr-%d-%d-%d", workerId, rTask, taskId)
		ofile, _ := os.Create(filename)

		intermediateFilesMapper[rTask] = append(intermediateFilesMapper[rTask], filename)

		for _, v := range valueList {
			fmt.Fprintf(ofile, "%v %v\n", k, v)
		}
		ofile.Close()
	}

	return intermediateFilesMapper
}

func executeReduceTask() {

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
