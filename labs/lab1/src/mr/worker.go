package mr

import (
	"fmt"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//use ihash(key) % NReduce to choose the reduce
//task number for each KeyValue emitted by Map.
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

	for {
		args := Args{}
		reply := Reply{}

		args.ReqType = AskForTask
		res := callMaster(&args, &reply)

		if res {
			if reply.Status == TaskAvailable {
				switch reply.TaskType {
				case MapTask:
					result := DoMap(reply.Filename, reply.TaskId, reply.ReduceTasks, mapf)
					if result {
						finishTask(&args, &reply, reply.TaskId)
					}
				case ReduceTask:
					result := DoReduce(reply.MapTasks, reply.TaskId, reducef)
					if result {
						finishTask(&args, &reply, reply.TaskId)
					}
				default:
					log.Println("Unknown task type!")
					time.Sleep(time.Second)
				}
			} else if reply.Status == AllTaskDone {
				break
			} else {
				time.Sleep(time.Second)
			}
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()
}

// calls master's worker handler function
func callMaster(args *Args, reply *Reply) bool {
	return call("Master.WorkerHandler", args, reply)
}

// calls mater's worker handler function, tells master that a task has been done
func finishTask(args *Args, reply *Reply, taskId int) {
	args.TaskId = taskId
	args.ReqType = FinishTask
	if !callMaster(args, reply) {
		log.Println("Error sending result to master")
	}
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
