package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
)

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)

}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	workerId := os.Getpid()
	log.Printf("Worker %d: Start\n", workerId)
	lastTaskId := -1
	lastTaskType := FREE
	finished := false

	for {
		args := TaskArgs{
			WorkerId:     workerId,
			LastTaskId:   lastTaskId,
			LastTaskType: lastTaskType,
			Finished:     finished,
		}
		reply := TaskReply{}
		ok := call("Coordinator.AskForTask", &args, &reply)
		if !ok {
			log.Printf("Worker %d: RPC Error!\n", workerId)
			return
		}

		switch reply.TaskType {
		case MAP:
			//TODO:map
			finished = doMap(mapf, args, reply)
			if finished {
				log.Printf("Worker %d: %s Task %d done\n", workerId, reply.TaskType, reply.TaskId)
			} else {
				log.Printf("Worker %d: %s Task %d failed\n", workerId, reply.TaskType, reply.TaskId)
			}
		case REDUCE:
			//TODO:reduce
			finished = doReduce(reducef, args, reply)
		case FREE:
			finished = false
		case CLOSE:
			return
		}

		lastTaskId = reply.TaskId
		lastTaskType = reply.TaskType
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doReduce(reducef func(string, []string) string, args TaskArgs, reply TaskReply) bool {
	var contents []string
	for i := 0; i < reply.MapNum; i++ {
		filename := getFinalMapDir(i, reply.TaskId)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return false
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
			return false
		}
		file.Close()
		contents = append(contents, strings.Split(string(content), "\n")...)
	}

	var kva []KeyValue
	for _, line := range contents {
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		kva = append(kva, KeyValue{
			Key:   parts[0],
			Value: parts[1],
		})
	}

	sort.Sort(ByKey(kva))
	os.Mkdir("reduce", os.ModePerm)
	ofile, _ := os.Create(getTempReduceDir(args.WorkerId, reply.TaskId))

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()
	return true
}

func doMap(mapf func(string, string) []KeyValue, args TaskArgs, reply TaskReply) bool {
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
		return false
	}
	file.Close()
	kva := mapf(reply.FileName, string(content))
	temp_out_map := make(map[int][]KeyValue)
	for _, kv := range kva {
		temp_out_map[ihash(kv.Key)%reply.ReduceNum] = append(temp_out_map[ihash(kv.Key)%reply.ReduceNum], kv)
	}
	os.Mkdir("map", os.ModePerm)
	for i := 0; i < reply.ReduceNum; i++ {
		tempFile, _ := os.Create(getTempMapDir(args.WorkerId, reply.TaskId, i))
		for _, kv := range temp_out_map[i] {
			fmt.Fprintf(tempFile, "%v\t%v\n", kv.Key, kv.Value)
		}
		tempFile.Close()
	}
	return true
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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

func getTempMapDir(workerId int, mapId int, reduceId int) string {
	return fmt.Sprintf("map/temp-%d-%d-%d", workerId, mapId, reduceId)
}

func getFinalMapDir(mapId int, reduceId int) string {
	return fmt.Sprintf("map/final-%d-%d", mapId, reduceId)
}

func getTempReduceDir(workerId int, reduceId int) string {
	return fmt.Sprintf("reduce/temp-%d-out-%d", workerId, reduceId)
}

func getOutDir(reduceId int) string {
	return fmt.Sprintf("mr-out-%d", reduceId)
}
