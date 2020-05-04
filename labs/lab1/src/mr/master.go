package mr

import (
	"container/list"
	"fmt"
	"log"
	"strconv"
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
func (m *Master) WorkerHandler(args *Args, reply *Reply) error {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		if args.ReqType == AskForTask {
			m.assignTask(reply)
		} else {
			m.finishTask(args)
		}
		wg.Done()
	}()
	wg.Wait()
	return nil
}

// handles workers' request for a task
func (m *Master) assignTask(reply *Reply) {
	status := m.status
	if status == MapStage {
		m.assignMapTask(reply)
	} else if status == ReduceStage {
		m.assignReduceTask(reply)
	} else {
		reply.Status = AllTaskDone
	}
}

// assign a map task to worker
func (m *Master) assignMapTask(reply *Reply) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mapNotAssigned.Len() > 0 {
		element := m.mapNotAssigned.Front()
		index := element.Value.(int)
		reply.Status = TaskAvailable
		reply.TaskId = index
		reply.TaskType = MapTask
		reply.ReduceTasks = m.reduceTasks
		reply.Filename = m.mapTasks[index]
		m.mapInProgress[index] = setValue
		m.mapNotAssigned.Remove(element)
		log.Println("assigned 1 reduce task, task id: " + strconv.Itoa(reply.TaskId))
		go m.checkTask(MapTask, index)
	} else {
		reply.Status = NoTaskAvailable
	}
}

// assign a reduce task to worker
func (m *Master) assignReduceTask(reply *Reply) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reduceNotAssigned.Len() > 0 {
		element := m.reduceNotAssigned.Front()
		index := element.Value.(int)
		reply.Status = TaskAvailable
		reply.TaskType = ReduceTask
		reply.TaskId = index
		reply.MapTasks = len(m.mapTasks)
		m.reduceInProgress[index] = setValue
		m.reduceNotAssigned.Remove(element)
		log.Println("assigned 1 map task")
		go m.checkTask(ReduceTask, index)
	} else {
		reply.Status = NoTaskAvailable
	}
}

// handles finish task request from a worker
func (m *Master) finishTask(args *Args) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.status == MapStage {
		delete(m.mapInProgress, args.TaskId)
		m.mapFinished++
		if m.mapFinished == len(m.mapTasks) {
			m.status = ReduceStage
		}
		fmt.Println("finished 1 map task")
	} else if m.status == ReduceStage {
		delete(m.reduceInProgress, args.TaskId)
		m.reduceFinished++
		if m.reduceFinished == m.reduceTasks {
			m.status = FinishedStage
		}
		m.deleteIntermediates(args.TaskId)
		fmt.Println("finished 1 reduce task")
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
	if m.reduceFinished == m.reduceTasks {
		ret = true
	}

	return ret
}

//
// check if a task is successfully finished in 10 seconds
// this would be call every time when a task is assigned to a worker
//
func (m *Master) checkTask(taskType int, taskId int) {
	time.Sleep(time.Second * 10)
	if taskType == MapTask {
		_, exists := m.mapInProgress[taskId]
		if exists {
			m.mapNotAssigned.PushBack(taskId)
			log.Println("1 map task failed")
		}
	} else if taskType == ReduceTask {
		_, exists := m.reduceInProgress[taskId]
		if exists {
			m.reduceNotAssigned.PushBack(taskId)
			log.Println("1 reduce task failed")
		}
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mapTasks = files
	m.reduceTasks = nReduce
	m.status = MapStage
	m.reduceFinished = 0
	m.mapFinished = 0
	m.mapInProgress = make(map[int]void)
	m.reduceInProgress = make(map[int]void)
	for i := 0; i < len(files); i++ {
		m.mapNotAssigned.PushBack(i)
	}
	for i := 0; i < nReduce; i++ {
		m.reduceNotAssigned.PushBack(i)
	}

	m.server()
	return &m
}

// deletes intermediate files
func (m *Master) deleteIntermediates(reduceId int) {
	for i := 0; i < len(m.mapTasks); i++ {
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceId)
		_ = os.Remove(filename)
	}
}
