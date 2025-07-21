package kvsrv

import (
	// "fmt"
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu    sync.Mutex
	kvmap sync.Map
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	if val, ok := kv.kvmap.Load(args.Key); ok {
		// 类型断言
		if storedArgs, ok := val.(*rpc.PutArgs); ok {
			reply.Value = storedArgs.Value
			reply.Version = storedArgs.Version
			reply.Err = rpc.OK
		}
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if val, ok := kv.kvmap.Load(args.Key); ok {
		// 类型断言
		if storedArgs, ok := val.(*rpc.PutArgs); ok {
			if storedArgs.Version == args.Version { // 版本相等
				// fmt.Println("verison match")
				
				args.Version++
				// fmt.Println("put the old key:", args.Key, ",vaule:", args.Value, ",ver:", args.Version)
				kv.kvmap.Store(args.Key, args)
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrVersion
				// fmt.Println("verison mismatch, cache:", storedArgs.Version, ",incoming:", args.Version)
			}
		}
	} else {
		if args.Version > 0 {
			reply.Err = rpc.ErrNoKey
		}else{
			args.Version++
			// fmt.Println("put the init key:", args.Key, ",vaule:", args.Value, ",ver:", args.Version)
			kv.kvmap.Store(args.Key, args)
			reply.Err = rpc.OK
		}
	}
	
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
