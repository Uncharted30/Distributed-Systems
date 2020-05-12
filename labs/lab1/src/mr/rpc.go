package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// request types
const AskForTask = 1
const FinishTask = 2

// master status types
const TaskAvailable = 3
const NoTaskAvailable = 4
const AllTaskDone = 5

// task types
const MapTask = 6
const ReduceTask = 7

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type Args struct {
	// request type, 1: ask for a task, 2: a task is finished
	ReqType int

	TaskId int
}

type Reply struct {

	// master status, 3: task available, 4: no task available, 5: all tasks are done
	Status int

	// task type, 6: map task, 7: reduce task
	TaskType int

	// input file
	Filename string

	// number of map tasks
	MapTasks int

	// number of reduce tasks
	ReduceTasks int

	TaskId int
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
