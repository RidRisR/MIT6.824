package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	TaskMap    map[int]*TaskInfo
	dispatcher chan TaskReply
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

	// Your code here.

	return (c.Stage == CLOSE)
}

func (c *Coordinator) AskForTask(args *TaskArgs, reply *TaskReply) error {
	c.checkLastTask(args)

	if len(c.dispatcher) != 0 {
		*reply = <-c.dispatcher
		c.TaskMap[reply.TaskId] = &TaskInfo{
			WorkerId: args.WorkerId,
			TaskId:   reply.TaskId,
			TaskType: reply.TaskType,
			FileName: reply.FileName,
			Expire:   time.Now().Add(10 * time.Second),
		}
		return nil
	}

	if len(c.TaskMap) == 0 {
		if c.Stage == MAP {
			c.Stage = REDUCE
			c.changeStage()
			*reply = TaskReply{
				TaskId:   -1,
				TaskType: FREE,
			}
		}
		if c.Stage == REDUCE {
			c.Stage = CLOSE
			close(c.dispatcher)
		}
	}

	return nil
}

func (c *Coordinator) checkLastTask(args *TaskArgs) {
	if args.LastTaskId == -1 || args.LastTaskType == FREE {
		return
	}
	lastTask := c.TaskMap[args.LastTaskId]
	if lastTask.WorkerId == args.WorkerId && args.Finished {
		log.Printf("Task %d: checked", args.LastTaskId)
		go commit(args.WorkerId, lastTask.TaskId, lastTask.TaskType, c.ReducerNum)
		delete(c.TaskMap, args.LastTaskId)
	}
	if lastTask.WorkerId == args.WorkerId && !args.Finished {
		log.Printf("Task %d: re-dispatch", args.LastTaskId)
		c.dispatcher <- TaskReply{
			TaskId:   lastTask.TaskId,
			TaskType: lastTask.TaskType,
			FileName: lastTask.FileName,
		}
	}
	if lastTask.WorkerId != args.WorkerId && args.Finished {
		log.Printf("Task %d: wrong worker. worker %d submitted, it should be worker %d", args.LastTaskId, args.WorkerId, lastTask.WorkerId)
		for i := 0; i < c.ReducerNum; i++ {
			dirToRemove := getTempMapDir(args.WorkerId, args.LastTaskId, i)
			err := os.Remove(dirToRemove)
			if err != nil {
				log.Fatalf("Task %d: cannot remove %s", lastTask.TaskId, dirToRemove)
				return
			}
		}
	}
}

func commit(workerId int, taskId int, taskType string, nReduce int) {
	if taskType == MAP {
		for i := 0; i < nReduce; i++ {
			oldPath := getTempMapDir(workerId, taskId, i)
			newPath := getFinalMapDir(taskId, i)
			err := os.Rename(oldPath, newPath)
			if err != nil {
				log.Fatalf(
					"Failed to mark map output file as final: %e", err)
			}
		}
	}
	if taskType == REDUCE {
		err := os.Rename(
			getTempReduceDir(workerId, taskId),
			getOutDir(taskId))
		if err != nil {
			log.Fatalf(
				"Failed to mark reduce output file  as final: %e", err)
		}
	}
}

func (c *Coordinator) changeStage() {
	c.TaskMap = make(map[int]*TaskInfo)
	for i := 0; i < c.ReducerNum; i++ {
		c.dispatcher <- TaskReply{
			TaskId:    i,
			TaskType:  REDUCE,
			FileName:  strconv.Itoa(i),
			ReduceNum: c.ReducerNum,
			MapNum:    c.MapNum,
		}
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
		TaskMap:    make(map[int]*TaskInfo),
		dispatcher: make(chan TaskReply, len_dispatcher),
		Stage:      MAP,
		ReducerNum: nReduce,
		MapNum:     len(files),
	}

	for i, file := range files {
		c.dispatcher <- TaskReply{
			TaskId:    i,
			TaskType:  MAP,
			FileName:  file,
			ReduceNum: nReduce,
			MapNum:    len(files),
		}
	}

	// Your code here.
	log.Printf("Coordinator start\n")
	c.server()
	return &c
}
