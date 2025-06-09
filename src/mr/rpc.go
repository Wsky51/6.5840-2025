package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
    X int
}

type ExampleReply struct {
    Y int
}

type TaskReqArgs struct {
    DoneTaskId int // 已完成任务的id
    DoneTaskType int // 已完成任务的类型
    IntermediateFiles []string //生成的中间文件(Map任务完成时需要)
}

type Task struct {
    FileName []string // 文件名
    TaskId int // 任务ID
    TaskType int // 任务类型 1:map, 2:reduce, 3:heartbeat
    NReduce int // reduce个数
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
    s := "/var/tmp/5840-mr-"
    s += strconv.Itoa(os.Getuid())
    return s
}