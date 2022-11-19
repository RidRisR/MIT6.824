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
	TaskMap    map[int]*TaskInfo
	Dispatcher chan TaskReply
	Stage      string
	ReducerNum int
	MapNum     int
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
	checked := false
	if args.LastTaskId != -1 {
		lastTask := c.TaskMap[args.LastTaskId]
		if lastTask.WorkerId == args.WorkerId && args.Finished {
			log.Printf("Task %d: checked", args.LastTaskId)
			checked = true
			delete(c.TaskMap, args.LastTaskId)
		}
		if lastTask.WorkerId == args.WorkerId && !args.Finished {
			log.Printf("Task %d: failed", args.LastTaskId)
			c.Dispatcher <- TaskReply{
				TaskId:   lastTask.TaskId,
				TaskType: lastTask.TaskType,
				FileName: lastTask.FileName,
			}
		}
	}

	if len(c.Dispatcher) != 0 {
		*reply = <-c.Dispatcher
		reply.Checked = checked
		if !checked {
			log.Printf("Task %d: re-dispatched", reply.TaskId)
		}
		c.TaskMap[reply.TaskId] = &TaskInfo{
			WorkerId: args.WorkerId,
			TaskId:   reply.TaskId,
			TaskType: reply.TaskType,
			FileName: reply.FileName,
			Expire:   time.Now().Add(10 * time.Second),
		}
	}

	if len(c.TaskMap) == 0 {
		log.Printf("AAAAA!!!!! ")
	}

	return nil
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
		TaskMap:    make(map[int]*TaskInfo),
		Dispatcher: make(chan TaskReply, len_dispatcher),
		Stage:      MAP,
		ReducerNum: nReduce,
		MapNum:     len(files),
	}

	for i, file := range files {
		c.Dispatcher <- TaskReply{
			TaskId:    i,
			TaskType:  MAP,
			FileName:  file,
			ReduceNum: nReduce,
			Checked:   false,
		}
	}

	// Your code here.
	log.Printf("Coordinator start\n")
	c.server()
	return &c
}
