package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskState int

const (
	Idle TaskState = 0
	InProgress TaskState = 1
	Completed	 TaskState = 2
	Failed TaskState = 3
)

type Phase int

const (
	MapPhase Phase = 0
	ReducePhase Phase = 1
	DonePhase Phase = 2
)

type Coordinator struct {
	// Your definitions here.
	inputFiles []string
	// states of tasks
	mapTasks []TaskState
	reduceTasks []TaskState
	// number of reduce tasks
	nReduce int
	// start time of each task, used for timeout detection
	mapTaskStartTime []time.Time
	reduceTaskStartTime []time.Time
	// current phase of the job map/reduce/done
	phase Phase

	// mutex lock for workers to access the coordinator's state
	coordinaterLock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.coordinaterLock.Lock()
	defer c.coordinaterLock.Unlock()
	switch c.phase {

	// find an idle map task first
	case MapPhase:
		for i, state := range c.mapTasks {
			if state == Idle {
				c.mapTasks[i] = InProgress
				c.mapTaskStartTime[i] = time.Now()
				reply.TaskType = MapTask
				reply.TaskId = i
				reply.FileName = c.inputFiles[i]
				reply.ReduceN = c.nReduce
				return nil
			}
		}
		// if no idle task, ask worker to wait
		reply.TaskType = Wait
		return nil

	// if no idle map task, find an idle reduce task
	case ReducePhase:
		for i, state := range c.reduceTasks {
			if state == Idle {
				c.reduceTasks[i] = InProgress
				c.reduceTaskStartTime[i] = time.Now()
				reply.TaskType = ReduceTask
				reply.TaskId = i
				reply.MapM = len(c.inputFiles)
				return nil
			}
		}
		// if no idle task, ask worker to wait
		reply.TaskType = Wait
		return nil

	// if no idle map/reduce task, ask worker to exit
	case DonePhase:
		reply.TaskType = Exit
		return nil
	}
	// if no idle task, ask worker to wait
	return fmt.Errorf("invalid phase")
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
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

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.inputFiles = files
	c.nReduce = nReduce
	c.mapTasks = make([]TaskState, len(files))
	c.reduceTasks = make([]TaskState, nReduce)
	c.mapTaskStartTime = make([]time.Time, len(files))
	c.reduceTaskStartTime = make([]time.Time, nReduce)
	c.phase = MapPhase

	// coordinator listens for RPCs from workers
	c.server(sockname)
	return &c
}
