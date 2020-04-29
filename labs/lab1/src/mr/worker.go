package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
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

	for {
		args.reqType = AskForTask
		res := callMaster(&args, &reply)

		if res {
			if reply.status == TaskAvailable {
				switch reply.taskType {
				case MapTask:
					result := doMap(reply.filename, reply.taskId, 10, mapf)
					if result {
						finishTask(&args, &reply, reply.taskId)
					}
				case ReduceTask:
					result := doReduce(reply.mapTasks, reply.taskId, reducef)
					if result {
						finishTask(&args, &reply, reply.taskId)
					}
				default:
					log.Println("Unknown task type!")
					time.Sleep(time.Second * 5)
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
	return call("Master.WorkerHandler", &args, &reply)
}

func finishTask(args *Args, reply *Reply, taskId int) {
	args.taskId = taskId
	if !callMaster(args, reply) {
		log.Println("Error sending result to master")
	}
}

func doMap(filename string, taskId int, reduceTasks int, mapf func(string, string) []KeyValue) bool {
	content := readFile(filename)
	kva := mapf(filename, content)
	sort.Sort(ByKey(kva))
	intermediateMap := make(map[int][][]KeyValue)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		key := kva[i].Key
		var keyValues []KeyValue
		for k := i; k < j; k++ {
			keyValues = append(keyValues, kva[i])
		}

		reduceId := ihash(key) % reduceTasks
		intermediateMap[reduceId] = append(intermediateMap[reduceId], keyValues)

		i = j
	}

	result := toJson(taskId, intermediateMap)

	return result
}

func doReduce(mapTasks int, taskId int, reducef func(string, []string) string) bool {
	out, err := os.Create("mr-out-" + strconv.Itoa(taskId))
	if err != nil {
		log.Printf("Reduce task %d error creating output file.", taskId)
		return false
	}

	var kva []KeyValue

	for i := 0; i < mapTasks; i++ {
		kva, _ = decodeJson(i, taskId, kva)
	}

	sort.Sort(ByKey(kva))

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		key := kva[i].Key
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[i].Value)
		}

		result := reducef(key, values)
		_, err := fmt.Fprintf(out, "%v %v\n", key, result)
		if err != nil {
			log.Printf("Reduce task %d rror output result.\n", taskId)
			return false
		}

		i = j
	}

	out.Close()
	return true
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

func toJson(taskId int, intermediateMap map[int][][]KeyValue) bool {
	for key := range intermediateMap {
		filename := "mr-" + strconv.Itoa(taskId) + "-" + strconv.Itoa(key)

		file, err := ioutil.TempFile("", "tmp-*")
		if err != nil {
			log.Println("Error creating temp file.")
			return false
		}

		enc := json.NewEncoder(file)
		for _, keyValues := range intermediateMap[key] {
			for _, keyValue := range keyValues {
				err := enc.Encode(keyValue)
				if err != nil {
					log.Println("Cannot decode key value: " + keyValue.Key + keyValue.Value + " to Json")
				}
			}
		}

		_, err = os.Stat(filename)

		if err == nil || !os.IsNotExist(err) {
			return false
		}

		err = os.Rename(file.Name(), filename)
		if err != nil {
			_ = os.Remove(file.Name())
			log.Println("Failed to rename file")
			return false
		}

		_ = file.Close()
	}

	return true
}

func decodeJson(index int, taskId int, kva []KeyValue) ([]KeyValue, bool) {
	filename := "mr-" + strconv.Itoa(index) + "-" + strconv.Itoa(taskId)
	file, err := os.Open(filename)

	if err != nil {
		log.Println("File " + filename + " does not exits.")
		return nil, false
	}

	dec := json.NewDecoder(file)

	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}

	return kva, true
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
