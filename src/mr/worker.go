package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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
	//CallExample()
	idArgs := WorkerIdReply{}
	//args := ExampleArgs{}
	call("Master.RegisterWorker", "", &idArgs)
	workerId := idArgs.Id
	fmt.Println("Worker Id:", workerId)

	for {
		request := GetTaskRequest{}
		reply := GetTaskReply{}

		call("Master.GetTask", &request, &reply)

		if reply.TaskType == MapTask {
			fmt.Println("Received Map Task with Id:", reply.TaskId)
			result := executeMapTask(workerId, reply.TaskId, reply.Filenames, reply.NumReduce, mapf)

			doneRequest := TaskCompletedRequest{}
			doneReply := TaskCompletedReply{}

			doneRequest.TaskId = reply.TaskId
			doneRequest.FilenamesMapper = result
			doneRequest.TaskType = MapTask
			doneRequest.WorkerId = workerId

			call("Master.WorkerTaskCompleted", &doneRequest, &doneReply)

		} else if reply.TaskType == ReduceTask {
			fmt.Println("Received Reduce Task with Id:", reply.TaskId)
			result := executeReduceTask(0, reply.Filenames, reducef)

			doneRequest := TaskCompletedRequest{}
			doneReply := TaskCompletedReply{}

			doneRequest.TaskId = reply.TaskId
			doneRequest.ReduceOutputFile = result
			doneRequest.TaskType = ReduceTask
			doneRequest.WorkerId = workerId

			call("Master.WorkerTaskCompleted", &doneRequest, &doneReply)
		}
		break
		//time.Sleep(time.Millisecond * 50)
	}

	// uncomment to send the Example RPC to the master.
	//CallExample()
}

func executeMapTask(
	workerId int,
	taskId int,
	filenames []string,
	numReduce int,
	mapf func(string, string) []KeyValue) map[int]string {

	intermediateFilesMapper := make(map[int]string)
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

	mapper := make(map[int][]KeyValue)

	for _, item := range kvTotal {
		rTaskIndex := ihash(item.Key) % numReduce
		mapper[rTaskIndex] = append(mapper[rTaskIndex], item)
	}

	for rTaskIndex, kvList := range mapper {
		filename := fmt.Sprintf("mr-intermediate-%d-%d", rTaskIndex, taskId)
		ofile, _ := os.Create(filename)

		intermediateFilesMapper[rTaskIndex] = filename

		for _, item := range kvList {
			fmt.Fprintf(ofile, "%v %v\n", item.Key, item.Value)
		}
		ofile.Close()
	}

	return intermediateFilesMapper
}

func executeReduceTask(
	reduceTaskIndex int,
	filenames []string,
	reducef func(string, []string) string) string {

	keyToValues := make(map[string][]string)

	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		scanner := bufio.NewScanner(file)
		var key string
		var value string

		for scanner.Scan() {
			line := scanner.Text()
			fmt.Sscanf(line, "%s %s\n", &key, &value)
			keyToValues[key] = append(keyToValues[key], value)
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}

		file.Close()
	}
	filename := fmt.Sprintf("mr-out-%d", reduceTaskIndex)
	ofile, _ := os.Create(filename)

	for k, v := range keyToValues {
		output := reducef(k, v)
		fmt.Fprintf(ofile, "%v %v\n", k, output)
	}
	ofile.Close()

	return filename
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
