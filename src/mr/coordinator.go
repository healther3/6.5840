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
				reply.TaskType := MapTask
				reply.TaskId := i
				reply.FileName := c.inputFiles[i]
				reply.ReduceN := c.nReduce
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
				reply.TaskType := ReduceTask
				reply.TaskId := i
				reply.MapM := len(c.inputFiles)
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
	// set mutex lock to coordinator
	c.coordinaterLock.Lock()
	defer c.coordinaterLock.Unlock()
	taskType := args.TaskType
	TaskId := args.TaskId
	success := args.Success

	// if task is completed successfully, update the state of the task and decide whether to move to the next phase
	if success {
		switch taskType {
		case MapTask:
			c.mapTasks[TaskId] = Completed
			// if all map tasks are completed, move to reduce phase
			for _, state := range c.mapTasks {
				if state != Completed {
					return nil
				}
			}
			c.phase = ReducePhase
			return nil
		case ReduceTask:
			c.reduceTasks[TaskId] = Completed
			// if all reduce tasks are completed, move to done phase
			for _, state := range c.reduceTasks {
				if state != Completed {
					return nil
				}
			}
			c.phase = DonePhase
		}
	} else {
		// if task is failed, update the state of the task to idle for retrying
		switch taskType {
		case MapTask:
			c.mapTasks[TaskId] = Idle
			return nil
		case ReduceTask:
			c.reduceTasks[TaskId] = Idle
			return nil
		}

	return fmt.Errorf("invalid phase")
	}
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
	// prevents workers from accessing the coordinator's state 
	// while the coordinator is checking whether the job is done (data race prevention)
	c.coordinaterLock.Lock()
	defer c.coordinaterLock.Unlock()
	return c.phase == DonePhase
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

	// check for timeout tasks and reset them to idle for retrying
	go func ()  {
		for {
			time.Sleep(time.Second)
			c.coordinaterLock.Lock()

			// if the job is done, exit the goroutine
			if c.phase == DonePhase {
				c.coordinaterLock.Unlock()
				return
			}

			switch c.phase {
				// if the job is in map/reduce phase, check for timeout tasks and reset them to idle for retrying
			case MapPhase:
				for i, state := range c.mapTasks {
					if state == InProgress {
						if time.Since(c.mapTaskStartTime[i]) > 10*time.Second {
							c.mapTasks[i] = Idle
						}
					}
				}
			case ReducePhase:
				// if the job is in reduce phase, check for timeout tasks and reset them to idle for retrying
				for i, state := range c.reduceTasks {
					if state == InProgress {
						if time.Since(c.reduceTaskStartTime[i]) > 10*time.Second {
							c.reduceTasks[i] = Idle
						}
					}
				}
			}
			c.coordinaterLock.Unlock()
		}
	} ()
	return &c
}
