package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	TaskMap    map[int]TaskInfo
	Dispatcher chan TaskReply
	Stage      string
	ReducerNum int
	MapNum     int
	TaskNum    int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func (c *Coordinator) AskForTask(args *TaskArgs, reply *TaskReply) error {
	lastTask := c.TaskMap[args.LastTaskId]
	if args.LastTaskId != -1 {
		if lastTask.WorkerId == args.WorkerId && lastTask.finished == false {
			c.TaskNum -= 1
			//TODO:commit
		}
	}

	if len(c.Dispatcher) != 0 {
		*reply = <-c.Dispatcher
		c.TaskMap[reply.TaskId] = TaskInfo{
			WorkerId: args.WorkerId,
			TaskId:   reply.TaskId,
			TaskType: reply.TaskType,
			FileName: reply.FileName,
			Expire:   time.Now().Add(10 * time.Second),
			finished: false,
		}
	}

	if c.TaskNum == 0 {
		//TODO:Change Stage
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	len_dispatcher := len(files)
	if len(files) < nReduce {
		len_dispatcher = nReduce
	}

	c := Coordinator{
		TaskMap:    make(map[int]TaskInfo),
		Dispatcher: make(chan TaskReply, len_dispatcher),
		Stage:      MAP,
		ReducerNum: nReduce,
		MapNum:     len(files),
		TaskNum:    len(files),
	}

	for i, file := range files {
		task := TaskReply{
			TaskId:   i,
			TaskType: MAP,
			FileName: file,
		}
		c.Dispatcher <- task
	}

	// Your code here.
	log.Printf("Coordinator start\n")
	c.server()
	return &c
}
