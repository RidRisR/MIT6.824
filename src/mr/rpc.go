package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

const (
	FREE   = "FREE"
	MAP    = "MAP"
	REDUCE = "REDUCE"
)

// Add your RPC definitions here.

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskArgs struct {
	WorkerId     int
	LastTaskId   int
	LastTaskType string
	Finished     bool
}

type TaskReply struct {
	TaskId    int
	TaskType  string
	FileName  string
	ReduceNum int
	Checked   bool
}

type TaskInfo struct {
	WorkerId int
	TaskId   int
	TaskType string
	FileName string
	Expire   time.Time
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
