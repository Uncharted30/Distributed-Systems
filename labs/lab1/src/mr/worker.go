package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"


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

type Intermediate struct {
	Key string
	Values []string
}


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

	args := Args{}
	reply := Reply{}

	for  {
		res := callMaster(&args, &reply)
		args.reqType = AskForTask

		if res {
			if reply.status == TaskAvailable {
				if reply.taskType == MapTask {
					err := doMap(reply.filename, reply.taskId, mapf)
					if err != nil {
						panic(err)
					} else {
						args.reqType = FinishTask
						callMaster(&args, &reply)
					}
				} else if reply.taskType == ReduceTask {
					doReduce(reply.taskId, reply.taskId, reducef)
				}
			} else if reply.status == AllTaskDone {
				break
			}
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func callMaster(args *Args, reply *Reply) bool {
	return false
}

func doMap(filename string, taskId int, mapf func(string, string) []KeyValue) error {
	content := readFile(filename)
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))
	intermediateMap := make(map[int][]Intermediate)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		key := kva[i].Key
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		intermediate := Intermediate{
			Key: key,
			Values: values,
		}

		reduceId := ihash(key)
		intermediateMap[reduceId] = append(intermediateMap[reduceId], intermediate)
		i = j
	}

	err := toJson(taskId, intermediateMap)

	if err != nil {
		return err
	}

	return nil
}

func doReduce(mapTasks int, taskId int, reducef func(string, []string) string) {

}

func readFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	defer file.Close()
	return string(content)
}

func toJson(taskId int, intermediateMap map[int][]Intermediate) error {
	for key := range intermediateMap {
		file, err := os.Create("mr-" + strconv.Itoa(taskId) + "-" + strconv.Itoa(key))

		if err != nil {
			return err
		}

		enc := json.NewEncoder(file)
		err = enc.Encode(intermediateMap[key])
		if err != nil {
			return err
		}
	}

	return nil
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
