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

			// Prepare intermediate files for reduce tasks
			intermediateFiles := make([]*os.File, reply.NReduce)
			encoders := make([]*json.Encoder, reply.NReduce)

			for i := 0; i < reply.NReduce; i++ {
				intermediateFileName := os.createTemp("", "mr-tmp-*")
				intermediateFile, err := os.Create(intermediateFileName)
				if err != nil {
					log.Fatalf("cannot create intermediate file %v", intermediateFileName)
				}
				intermediateFiles[i] = intermediateFile
				encoders[i] = json.NewEncoder(intermediateFile)
			}

			// write the kv pairs to temporary intermediate files
			for _, kv := range kva {
				// write the kv pair to intermediate file (using hash function to determine which reduce task it belongs to)
				reduceTaskNum := ihash(kv.Key) % reply.NReduce
				// encode the kv pair to intermediate file using json encoder
				enc := encoders[reduceTaskNum]
				err = enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode kv pair %v", kv)
				}
			}

			// finish writing intermediate files, rename them to final names
			for i := 0; i < reply.NReduce; i++ {
				// get the temporary intermediate file name and rename it to final intermediate file name
				tmpFile := intermediateFiles[i]
				tmpFileName := tmpFile.Name()
				// close the temporary intermediate file before renaming
				err = tmpFile.Close()
				if err != nil {
					log.Fatalf("cannot close intermediate file %v", tmpFileName)
				}
				// rename the temporary intermediate file to final intermediate file name
				intermediateFileName := fmt.Sprintf("mr-%d-%d", reply.TaskId, i)
				err = os.Rename(tmpFileName, intermediateFileName)
				if err != nil {
					log.Fatalf("cannot rename intermediate file %v to %v", tmpFileName, intermediateFileName)
				}
			}

		case ReduceTask:
			// read intermediate files and sort by key
			kvMap := []mr.KeyValue{}
			
			for i := 0; i < reply.MapM; i++ {
				// read each intermediate file and save the kv pairs to kvMap
				intermediateFileName := fmt.Sprintf("mr-%d-%d", i, reply.TaskNum)
				intermediateFile, err := os.Open(intermediateFileName)
				if err != nil {
					log.Fatalf("cannot open intermediate file %v", intermediateFileName)
				}
				dec := json.NewDecoder(intermediateFile)
				for {
					// decode the kv pair and save to kvMap
					var kv mr.KeyValue
					err = dec.Decode(&kv)
					if err != nil {
						break
					}
					kvMap = append(kvMap, kv)
				}
				intermediateFile.Close()
			}
			// sort the kv pairs by key
			sort.Sort(ByKey(kvMap))

			// create output file for reduce task
			oname := fmt.Sprintf("mr-out-%d", reply.TaskNum)
			ofile, err := os.Create(oname)
			if err != nil {
				log.Fatalf("cannot create output file %v", oname)
			}

			// call reducef on each distinct key in kvMap, and write the output to output file
			for i := 0; i < len(kvMap); {
				j := i + 1
				for j < len(kvMap) && kvMap[j].Key == kvMap[i].Key {
					j++
				}
				values := []string{}
				// collect the values for the same key
				for k := i; k < j; k++ {
					values = append(values, kvMap[k].Value)
				}
				output := reducef(kvMap[i].Key, values)
				
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kvMap[i].Key, output)
				i = j
			}

			ofile.Close()

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
