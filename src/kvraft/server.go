package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Type  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMapper        map[string]string
	indextoChMapper map[int]chan raft.ApplyMsg
}

// Time to wait for receiving confirmation from Raft peer
const APPLY_WAIT_MS = 500

func (kv *KVServer) waitForOpConfirmation(index int) bool {
	ch := make(chan raft.ApplyMsg, 1)

	kv.mu.Lock()
	kv.indextoChMapper[index] = ch
	kv.mu.Unlock()

	ret := false

	select {
	case <-ch:
		ret = true
	case <-time.After(APPLY_WAIT_MS * time.Millisecond):
	}

	kv.mu.Lock()
	delete(kv.indextoChMapper, index)
	kv.mu.Unlock()

	return ret
}

func (kv *KVServer) receiveApply() {
	for {
		msg := <-kv.applyCh
		// Cast interface to Op
		op := msg.Command.(Op)
		index := msg.CommandIndex

		DPrintf("[Server] Received Apply Msg of type %v for key %v\n", op.Type, op.Key)

		kv.mu.Lock()
		if op.Type == PUT {
			kv.kvMapper[op.Key] = op.Value
		} else if op.Type == APPEND {
			if curVal, ok := kv.kvMapper[op.Key]; ok {
				kv.kvMapper[op.Key] = curVal + op.Value
			} else {
				kv.kvMapper[op.Key] = op.Value
			}
		}

		if ch, ok := kv.indextoChMapper[index]; ok {
			ch <- msg
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[Server] Received Get for Key: %v\n", args.Key)
	op := Op{}
	op.Key = args.Key
	op.Type = GET

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	isApplied := kv.waitForOpConfirmation(index)
	ret := ""
	reply.Err = ErrWrongLeader

	if !isApplied {
		return
	}

	DPrintf("[Server] Getting Value for Key: %v\n", args.Key)

	kv.mu.Lock()
	ret, ok := kv.kvMapper[args.Key]
	kv.mu.Unlock()

	if ok {
		reply.Err = OK
		reply.Value = ret
		DPrintf("[Server] Key %v found value %v\n", args.Key, ret)
	} else {
		reply.Err = ErrNoKey
		DPrintf("[Server] Key %v no value found\n", args.Key)
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{}
	op.Key = args.Key
	op.Value = args.Value
	op.Type = args.Op

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = ErrWrongLeader
	isApplied := kv.waitForOpConfirmation(index)

	if isApplied {
		reply.Err = OK
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvMapper = make(map[string]string)
	kv.indextoChMapper = make(map[int]chan raft.ApplyMsg)

	go kv.receiveApply()

	return kv
}
