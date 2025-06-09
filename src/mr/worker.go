package mr

import (
    "encoding/json"
    "fmt"
    "hash/fnv"
    "io/ioutil"
    "log"
    "net/rpc"
    "os"
    "sort"
    "strconv"
    "time"
)

type ByKey []KeyValue

// for sorting by key.
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

    for {
        // step 1 : 请求coordinator分配任务
        args := TaskReqArgs{}
        reply := Task{}
        ReqTask(&args, &reply)
        if len(reply.FileName) == 0 {
            // 没有任务可以运行，sleep 1s
            time.Sleep(time.Second)
            continue
        }

        // fmt.Println("get new task:",reply)

        // step 2 : 判断任务类型并执行任务
        ExecTask(&reply, mapf, reducef)
    }

    // ReqMapTask(mapf)

    // uncomment to send the Example RPC to the coordinator.
    // CallExample()
}

// 执行任务
func ExecTask(reply *Task, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
    //exec map task
    if reply.TaskType == 1 {
        // fmt.Println("[worker] exec map task,", *reply)
        file, err := os.Open(reply.FileName[0])
        if err != nil {
            log.Fatalf("cannot open %v", reply.FileName)
        }
        content, err := ioutil.ReadAll(file)
        if err != nil {
            log.Fatalf("cannot read %v", reply.FileName)
        }
        file.Close()
        kva := mapf(reply.FileName[0], string(content))

        mapResult := make(map[int]([]KeyValue))
        for _, value := range kva { // 对key value做映射
            reduceNum := ihash(value.Key) % reply.NReduce
            mapResult[reduceNum] = append(mapResult[reduceNum], value)
        }

        intermediateFiles := make([]string, 0)
        // 写入中间文件
        for key, value := range mapResult {
            tmp_name := "mr-temp-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(key)
            file, err := os.Create(tmp_name)
            if err != nil {
                log.Fatalf("create file error: %v", err)
                return
            }
            defer file.Close()
            enc := json.NewEncoder(file)
            err = enc.Encode(value)
            if err != nil {
                log.Fatalf("write data to file: %v", err)
                return
            }
            intermediateFiles = append(intermediateFiles, tmp_name)
        }

        // 通知coordinator当前map 任务已经完成
        args := TaskReqArgs{
            DoneTaskId:        reply.TaskId,
            DoneTaskType:      reply.TaskType,
            IntermediateFiles: intermediateFiles}
        reply := Task{}
        NotifyTaskDone(&args, &reply)
    } else if reply.TaskType == 2 {
        // fmt.Println("[worker] exec reduce task,", *reply)
        intermediate := []KeyValue{}
        for _, temp_file := range reply.FileName {
            file, err := os.Open(temp_file)
            if err != nil {
                fmt.Println("open file error:", err)
                return
            }
            kva := []KeyValue{}
            dec := json.NewDecoder(file)
            err = dec.Decode(&kva)
            if err != nil {
                fmt.Println("JSON parse error:", err)
                return
            }
            file.Close()

            intermediate = append(intermediate, kva...)
        }
        sort.Sort(ByKey(intermediate))
        oname := "mr-out-" + strconv.Itoa(reply.TaskId)
        ofile, _ := os.Create(oname)
        i := 0
        for i < len(intermediate) {
            j := i + 1
            for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
                j++
            }
            values := []string{}
            for k := i; k < j; k++ {
                values = append(values, intermediate[k].Value)
            }
            output := reducef(intermediate[i].Key, values)

            // this is the correct format for each line of Reduce output.
            fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

            i = j
        }
        ofile.Close()
        // 通知coordinator当前map 任务已经完成
        args := TaskReqArgs{
            DoneTaskId:        reply.TaskId,
            DoneTaskType:      reply.TaskType,
            IntermediateFiles: make([]string, 0)}
        reply := Task{}
        ok := NotifyTaskDone(&args, &reply)
        // 删除中间文件
        if ok{
            for _, temp_file := range reply.FileName {
                os.Remove(temp_file)
            }
        }
    }
}

// 通知任务已经完成
func NotifyTaskDone(args *TaskReqArgs, reply *Task) bool{
    ok := call("Coordinator.TaskDone", args, reply)
    return ok
}

// rpc从coordinator获取任务
func ReqTask(args *TaskReqArgs, reply *Task) {
    ok := call("Coordinator.AllocateTask", args, reply)
    if !ok {
        fmt.Printf("call ReqTask failed!\n")
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
