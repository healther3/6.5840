package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"


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

var coordSockName string // socket for coordinator


// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
		for {
		// ask coordinator for a task
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		// if call failed, exit
		if !ok {
			log.Printf("call failed!")
			return
		}
		switch reply.TaskType {
		case MapTask:
			// read the input file
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", reply.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.FileName)
			}
			file.Close()
			kva := mapf(reply.FileName, string(content))
			// intermediate = append(intermediate, kva...)
			for _, kv := range kva {
				// write the kv pair to intermediate file (using hash function to determine which reduce task it belongs to)
				reduceTaskNum := ihash(kv.Key) % reply.NReduce
				// intermediate file name format: mr-X-Y, where X is the map task number and Y is the reduce task number
				intermediateFileName := fmt.Sprintf("mr-%d-%d", reply.TaskNum, reduceTaskNum)
				intermediateFile, err := os.Create(intermediateFileName)
				if err != nil {
					log.Fatalf("cannot create intermediate file %v", intermediateFileName)
				}
				// encode the kv pair to intermediate file using json encoder
				enc := json.NewEncoder(intermediateFile)
				err = enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode kv pair %v", kv)
				}
				intermediateFile.Close()
				// write the kv pair to intermediate file	
			}

		case ReduceTask:
			// read intermediate files and sort by key
		case Wait:
			// wait for a while and ask for a task again
		case Exit:
			// task finished exit the worker
			return
		}
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
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
