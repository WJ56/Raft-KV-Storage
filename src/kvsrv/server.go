package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type clientOperation struct {
	seq   int
	value string
}

type KVServer struct {
	mu sync.Mutex
	m  map[string]string
	// Your definitions here.
	duplicateTable map[int64]clientOperation
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value := kv.m[args.Key]
	reply.Value = value

	//currentOp, exists := kv.duplicateTable[args.Client]
	//if !exists || currentOp.seq < args.Seq {
	//	// 如果新的请求序列号更大，更新表中的条目
	//	value := kv.m[args.Key]
	//	reply.Value = value
	//	delete(kv.duplicateTable, args.Client)
	//	kv.duplicateTable[args.Client] = clientOperation{seq: args.Seq, value: value}
	//	return
	//}
	//if exists && currentOp.seq == args.Seq {
	//	reply.Value = currentOp.value
	//}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	currentOp, exists := kv.duplicateTable[args.Client]
	if !exists || currentOp.seq+1 == args.Seq {
		// 如果新的请求序列号更大，更新表中的条目
		kv.m[args.Key] = args.Value
		delete(kv.duplicateTable, args.Client)
		kv.duplicateTable[args.Client] = clientOperation{seq: args.Seq}
		return
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	currentOp, exists := kv.duplicateTable[args.Client]
	if !exists || currentOp.seq < args.Seq {
		// 如果新的请求序列号更大，更新表中的条目
		value := kv.m[args.Key]
		reply.Value = value
		kv.m[args.Key] += args.Value
		delete(kv.duplicateTable, args.Client)
		kv.duplicateTable[args.Client] = clientOperation{seq: args.Seq, value: value}
		return
	}
	if exists && currentOp.seq == args.Seq {
		reply.Value = currentOp.value
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.m = make(map[string]string)
	kv.duplicateTable = make(map[int64]clientOperation)
	return kv
}
