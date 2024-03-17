package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type clientOperation struct {
	Seq   int
	Value string
}

type operaInfo struct {
	clientId int64
	seqId    int
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string
	Key      string
	Value    string
	ClientId int64
	SeqId    int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// Your definitions here.
	duplicateTable map[int64]clientOperation
	m              map[string]string
	commandChan    map[operaInfo]chan int
	ps             *raft.Persister
}

type StateMachine struct {
	DuplicateTable map[int64]clientOperation
	M              map[string]string
}

func (kv *KVServer) apply() {
	for kv.killed() == false {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()
		if applyMsg.CommandValid == true {
			command := applyMsg.Command.(Op)
			currentOp, exists := kv.duplicateTable[command.ClientId]
			if !exists || currentOp.Seq < command.SeqId {
				// 执行
				// delete(kv.duplicateTable, command.ClientId)
				var value string
				switch command.Type {
				case "Get":
					value = kv.m[command.Key]
				case "Put":
					kv.m[command.Key] = command.Value
				case "Append":
					kv.m[command.Key] += command.Value
				}
				kv.duplicateTable[command.ClientId] = clientOperation{
					Seq:   command.SeqId,
					Value: value,
				}
				_, isLeader := kv.rf.GetState()
				commandChan, ok := kv.commandChan[operaInfo{clientId: command.ClientId, seqId: command.SeqId}]
				if isLeader == true && ok == true {
					commandChan <- 1
				}
				// TODO: snapshot
				if kv.maxraftstate != -1 && kv.ps.RaftStateSize() >= kv.maxraftstate {
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					e.Encode(StateMachine{
						DuplicateTable: kv.duplicateTable,
						M:              kv.m,
					})
					kv.rf.Snapshot(applyMsg.CommandIndex, w.Bytes())
				}
			}
		} else if applyMsg.SnapshotValid == true {
			r := bytes.NewBuffer(applyMsg.Snapshot)
			d := labgob.NewDecoder(r)
			var stateMachine StateMachine
			d.Decode(&stateMachine)
			kv.m = stateMachine.M
			kv.duplicateTable = stateMachine.DuplicateTable
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}

	currentOp, exists := kv.duplicateTable[args.Client]
	if !exists || currentOp.Seq < args.Seq {
		// 如果新的请求序列号更大，更新表中的条目
		op := Op{
			Type:     "Get",
			Key:      args.Key,
			Value:    "",
			ClientId: args.Client,
			SeqId:    args.Seq,
		}
		if _, _, isLeader := kv.rf.Start(op); isLeader == false {
			reply.Err = ErrWrongLeader
		} else {
			kv.commandChan[operaInfo{clientId: args.Client, seqId: args.Seq}] = make(chan int)
			commandChan := kv.commandChan[operaInfo{clientId: args.Client, seqId: args.Seq}]
			kv.mu.Unlock()
			select {
			case <-time.After(3000 * time.Millisecond):
				kv.mu.Lock()
				reply.Err = ErrWrongLeader
			case <-commandChan:
				kv.mu.Lock()
				value := kv.duplicateTable[args.Client].Value
				if value == "" {
					reply.Err = ErrNoKey
				} else {
					reply.Err = OK
					reply.Value = value
				}
			}
			close(commandChan)
			delete(kv.commandChan, operaInfo{clientId: args.Client, seqId: args.Seq})
		}
	} else if exists && currentOp.Seq == args.Seq {
		value := kv.duplicateTable[args.Client].Value
		if value == "" {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
			reply.Value = value
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// defer kv.mu.Unlock()
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, isLeader := kv.rf.GetState(); isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}
	// kv.mu.Lock()
	currentOp, exists := kv.duplicateTable[args.Client]
	// kv.mu.Unlock()
	if !exists || currentOp.Seq < args.Seq {
		// 如果新的请求序列号更大，更新表中的条目
		op := Op{
			Type:     args.Op,
			Key:      args.Key,
			Value:    args.Value,
			ClientId: args.Client,
			SeqId:    args.Seq,
		}
		if _, _, isLeader := kv.rf.Start(op); isLeader == false {
			reply.Err = ErrWrongLeader
		} else {
			kv.commandChan[operaInfo{clientId: args.Client, seqId: args.Seq}] = make(chan int)
			commandChan := kv.commandChan[operaInfo{clientId: args.Client, seqId: args.Seq}]
			kv.mu.Unlock()
			select {
			case <-time.After(2000 * time.Millisecond):
				kv.mu.Lock()
				reply.Err = ErrWrongLeader
			case <-commandChan:
				kv.mu.Lock()
				reply.Err = OK
				return
			}
			close(commandChan)
			delete(kv.commandChan, operaInfo{clientId: args.Client, seqId: args.Seq})
		}
	} else if exists && currentOp.Seq == args.Seq {
		reply.Err = OK
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//func (kv *KVServer) isLeader() {
//	for kv.killed() == false {
//		_, is := kv.rf.GetState()
//		if is == false {
//			// 遍历map中的每个键值对
//			for _, ch := range kv.commandChan {
//				// 检查通道是否为nil
//				if ch != nil {
//					// 如果通道非nil，关闭通道
//					close(ch)
//				}
//			}
//		}
//		time.Sleep(500 * time.Millisecond)
//	}
//}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to [marshall]/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.ps = persister
	if persister.SnapshotSize() != 0 {
		r := bytes.NewBuffer(persister.ReadSnapshot())
		d := labgob.NewDecoder(r)
		var stateMachine StateMachine
		d.Decode(&stateMachine)
		kv.m = stateMachine.M
		kv.duplicateTable = stateMachine.DuplicateTable
	}
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.m = make(map[string]string)
	kv.duplicateTable = make(map[int64]clientOperation)

	kv.commandChan = make(map[operaInfo]chan int)

	go kv.apply()

	// go kv.isLeader()

	return kv
}
