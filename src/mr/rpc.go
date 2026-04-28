package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskType int

const (
	MapTask TaskType = 0
	ReduceTask TaskType = 1
	Wait TaskType = 2
	Exit TaskType = 3
)

type GetTaskArgs struct {
	// ask for task need no information
}

type GetTaskReply struct {
	// map task or reduce task
	TaskType TaskType
	// #task
	TaskId int
	// number of reduce tasks, used for map tasks to hash
	ReduceN int
	// for reduce tasks, the number of map tasks, used for reduce tasks to read intermediate files
	MapM int
	// for map tasks, the input file name
	FileName string // for map tasks, the input file name
}

type ReportTaskArgs struct {
	// task type
	TaskType TaskType
	// task id
	TaskId int
	// whether the task is completed successfully
	Success bool
}

type ReportTaskReply struct {
	// no reply needed for now
}

