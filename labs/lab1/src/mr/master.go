package mr

import (
	"container/list"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type void struct{}

var setValue void

const MapStage = 1
const ReduceStage = 2
const FinishedStage = 3

type Master struct {
	// Your definitions here.
	mu sync.Mutex

	// filenames of map tasks
	mapTasks []string
	// map tasks wait to be assigned
	mapNotAssigned list.List
	// map tasks in progress
	mapInProgress map[int]void
	// number of finished map tasks
	mapFinished int

	// total number of reduce tasks
	reduceTasks int
	// reduce tasks wait to be assigned
	reduceNotAssigned list.List
	// reduce tasks in progress
	reduceInProgress map[int]void
	// number of finished reduce
	reduceFinished int

	// status of the master
	status int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) WorkerHandler(args *Args, reply *Reply) {
	if args.reqType == AskForTask {
		m.assignTask(reply)
	} else {

	}
}

// handles workers' request for a task
func (m *Master) assignTask(reply *Reply) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.status == MapStage {
		m.assignMapTask(reply)
	} else if m.status == ReduceStage {
		m.assignReduceTask(reply)
	} else {
		reply.status = AllTaskDone
	}
}

// assign a map task to worker
func (m *Master) assignMapTask(reply *Reply) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mapNotAssigned.Len() > 0 {
		element := m.mapNotAssigned.Front()
		index := element.Value.(int)
		reply.status = TaskAvailable
		reply.taskId = index
		reply.taskType = MapTask
		reply.filename = m.mapTasks[index]
		m.mapInProgress[index] = setValue
		m.mapNotAssigned.Remove(element)
		m.checkTask(MapTask, index)
	} else {
		reply.status = NoTaskAvailable
	}
}

// assign a reduce task to worker
func (m *Master) assignReduceTask(reply *Reply) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reduceNotAssigned.Len() > 0 {
		element := m.reduceNotAssigned.Front()
		index := element.Value.(int)
		reply.status = TaskAvailable
		reply.taskType = ReduceTask
		reply.taskId = index
		m.reduceInProgress[index] = setValue
		m.reduceNotAssigned.Remove(element)
		m.checkTask(ReduceTask, index)
	} else {
		reply.status = NoTaskAvailable
	}
}

// handles finish task request from a worker
func (m *Master) finishTask(args *Args) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.status == MapStage {
		m.finishMapTask(args)
	} else if m.status == ReduceStage {
		m.finishReduceTask(args)
	}
}

// finish a map task
func (m *Master) finishMapTask(args *Args) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.mapInProgress, args.taskId)
	m.mapFinished++
	if m.mapFinished == len(m.mapTasks) {
		m.status = ReduceStage
	}
}

// finish a reduce task
func (m *Master) finishReduceTask(args *Args) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.reduceInProgress, args.taskId)
	m.reduceFinished++
	if m.reduceFinished == m.reduceTasks {
		m.status = FinishedStage
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
// check if a task is successfully finished in 10 seconds
// this would be call every time when a task is assigned to a worker
//

func (m *Master) checkTask(taskType int, taskId int) {
	time.Sleep(time.Second * 10)

}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.server()
	return &m
}
