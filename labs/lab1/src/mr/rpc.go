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

const AskForTask = 1
const FinishTask = 2
const TaskAvailable = 3
const NoTaskAvailable = 4
const AllTaskDone = 5
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
	reqType int

	taskId int
}

type Reply struct {

	// master status, 3: task available, 4: no task available, 5: all tasks are done
	status int

	// task type, 6: map task, 7: reduce task
	taskType int

	// input files
	filename string

	// number of map tasks
	mapTasks int

	taskId int
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
