package mr

// https://blog.csdn.net/weixin_51322383/article/details/132068745
// https://juejin.cn/post/7260123819476926501#heading-13
import (
    // "fmt"
    "log"
    "net"
    "net/http"
    "net/rpc"
    "os"
    "strconv"
    "strings"
    "sync"
    "time"
)

type Coordinator struct {
    // Your definitions here.
    Files []string // 保存Map阶段生成的intermedate文件用
    NReduce int // reduce个数
    Stage int // 1: MAP阶段, 2: REDUCE阶段, 3: DONE所有任务已完成
    MapChan chan *Task  // Map任务channel
    ReduceChan chan *Task   // Reduce任务channel
    TaskState map[int]bool // 任务id是否已经完成，便于判断当前所处的阶段
    TaskStateMutex sync.Mutex //访问共享变量需加锁，确保线程安全
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
    reply.Y = args.X + 1
    return nil
}

// func (c *Coordinator) AllocateMapTask(args *TaskReqArgs, reply *TaskReplyArgs) error{
//  reply.File = c.Files[0];
//  return nil
// }

func (c *Coordinator) StageCheck(){
    // check if all map task finished
    if c.Stage == 1 {
        allMapFinish := true
        c.TaskStateMutex.Lock()
        for _, value := range c.TaskState {
            if !value{
                allMapFinish = false
                break
            }
        }
        if c.Stage == 1 && allMapFinish{
            c.Stage = 2
            // 准备分配Reduce任务
            reduce_base_idx := len(c.TaskState)
            arrFilterFunc := func(arr []string, suffix string) []string {
                var newArr []string
                for _, str := range arr {
                    if strings.HasSuffix(str, suffix) {
                        newArr = append(newArr, str)
                    }
                }
                return newArr
            }
            for i := 0; i < c.NReduce; i++ {
                task := Task {
                    FileName: arrFilterFunc(c.Files, strconv.Itoa(i)),
                    TaskId: reduce_base_idx + i,
                    TaskType: 2,
                    NReduce: c.NReduce,
                }
                if len(task.FileName) > 0{ //如果该reduce没有被分配任务就没必要加入到任务队列里面
                    c.TaskState[task.TaskId] = false
                    c.ReduceChan <- &task
                }
            }
        }
        c.TaskStateMutex.Unlock()
    }else if c.Stage == 2{
        allTaskFinish := true // include all map & reduce task
        c.TaskStateMutex.Lock()
        for _, value := range c.TaskState {
            if !value{
                allTaskFinish = false
                break
            }
        }
        c.TaskStateMutex.Unlock()
        if allTaskFinish{
            c.Stage = 3
        }
    }
}

func (c *Coordinator) TaskDone(args *TaskReqArgs, reply *Task) error{
    // intermediate file
    c.TaskStateMutex.Lock()
    if args.DoneTaskType ==1 && !c.TaskState[args.DoneTaskId]{
        c.Files = append(c.Files, args.IntermediateFiles...)
    }
    c.TaskState[args.DoneTaskId] = true
    c.TaskStateMutex.Unlock()
    // if args.DoneTaskType ==1{
    //  fmt.Println("[master] Map task:",args.DoneTaskId, "have done, args : ", args)
    // }else if args.DoneTaskType ==2{
    //  fmt.Println("[master] Reduce task:",args.DoneTaskId, "have done, args : ", args)
    // }

    return nil
}

func (c *Coordinator) AllocateTask(args *TaskReqArgs, reply *Task) error{
    c.StageCheck()
    // 处于map任务阶段
    if c.Stage == 1 {
        if (len(c.MapChan)!=0){
            *reply = *<-c.MapChan
            go c.TaskStateMonitor(reply)
        }
    }
    if c.Stage == 2 {
        if (len(c.ReduceChan)!=0){
            *reply = *<-c.ReduceChan
            go c.TaskStateMonitor(reply)
        }
    }
    return nil
}

//超时检测事件，
func (c *Coordinator) TaskStateMonitor(reply *Task) error{
    startTime := time.Now()
    for {
        c.TaskStateMutex.Lock()
        if c.TaskState[reply.TaskId]{ // 任务已经完成
            c.TaskStateMutex.Unlock()
            break
        }
        c.TaskStateMutex.Unlock()
        if time.Since(startTime) > 10*time.Second{
            // 超时, 重新加入到待分配channel中, 待分配给其他worker
            // fmt.Println("file timeout:", reply)
            if (reply.TaskType == 1 ){
                c.MapChan <- reply 
            }else if(reply.TaskType == 2 ){
                c.ReduceChan <- reply 
            }
            break
        }
        time.Sleep(time.Second)
    }
    return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
    ret := c.Stage == 3
    // Your code here.
    return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
    c := Coordinator{
        NReduce: nReduce, 
        Stage: 1, 
        MapChan: make(chan *Task, len(files)),
        ReduceChan: make(chan *Task, nReduce),
        TaskState: make(map[int]bool)}
    for id, file := range files {
        task := Task {
            FileName: []string{file},
            TaskId: id,
            TaskType: 1,
            NReduce: nReduce,
        }
        c.TaskState[id] = false
        c.MapChan <- &task
    }
    // close(c.MapFileCh)

    c.server()
    return &c
}
