package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

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
	Key       string
	Value     string
	Type      string
	RequestId int64
	ClientId  int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate     int // snapshot if log grows this big
	lastAppliedIndex int
	lastAppliedTerm  int

	// Your definitions here.
	kvMapper           map[string]string
	indextoChMapper    map[IndexId]chan raft.ApplyMsg
	clientsLastRequest map[int64]int64
}

type IndexId struct {
	Index     int
	RequestId int64
	ClientId  int64
}

// Time to wait for receiving confirmation from Raft peer
const APPLY_WAIT_MS = 500

func (kv *KVServer) waitForOpConfirmation(indexId IndexId) bool {
	ch := make(chan raft.ApplyMsg, 1)

	kv.mu.Lock()
	kv.indextoChMapper[indexId] = ch
	kv.mu.Unlock()

	ret := false

	select {
	case <-ch:
		ret = true
	case <-time.After(APPLY_WAIT_MS * time.Millisecond):
	}

	kv.mu.Lock()
	delete(kv.indextoChMapper, indexId)
	kv.mu.Unlock()

	return ret
}

func (kv *KVServer) checkNewSnapshotNeeded() {
	if kv.maxraftstate != -1 && kv.maxraftstate > kv.rf.GetRaftStateSize() {
		go func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()

			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)

			e.Encode(kv.lastAppliedIndex)
			e.Encode(kv.lastAppliedTerm)
			e.Encode(kv.kvMapper)
			e.Encode(kv.clientsLastRequest)

			data := w.Bytes()
			DPrintf("[Server] Sending Snapshot to Raft peer %d", kv.me)
			kv.rf.SaveSnapshotData(data, kv.lastAppliedIndex, kv.lastAppliedTerm)
		}()
	}
}

func (kv *KVServer) applySnapshot() {
	data := kv.rf.GetSnapshotData()
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var mapper map[string]string
	var lastReq map[int64]int64
	var lastIndex int
	var lastTerm int

	if d.Decode(&lastIndex) != nil ||
		d.Decode(&lastTerm) != nil ||
		d.Decode(&mapper) != nil ||
		d.Decode(&lastReq) != nil {
		log.Fatalf("[Server] Cannot Decode lastReq")
	} else {
		kv.mu.Lock()
		kv.kvMapper = mapper
		kv.clientsLastRequest = lastReq
		kv.lastAppliedIndex = lastIndex
		kv.lastAppliedTerm = lastTerm
		kv.mu.Unlock()
	}
}

func (kv *KVServer) receiveApply() {
	for {
		msg := <-kv.applyCh
		DPrintf("[Server] Received Apply Msg, isSnapshot %v\n", msg.IsSnapshot)
		if msg.IsSnapshot {
			go kv.applySnapshot()
			continue
		}
		// Cast interface to Op
		op := msg.Command.(Op)
		index := msg.CommandIndex
		term := msg.CommandTerm

		clientId := op.ClientId
		requestId := op.RequestId
		DPrintf("[Server] Received Apply Msg of type %v for key %v clientId %v reqId %v\n", op.Type, op.Key, clientId, requestId)

		kv.mu.Lock()
		kv.lastAppliedIndex = index
		kv.lastAppliedTerm = term

		ignore := false

		if clientLastRequest, found := kv.clientsLastRequest[clientId]; found {
			ignore = clientLastRequest >= requestId
		}

		if !ignore {
			kv.clientsLastRequest[clientId] = requestId
		}

		if op.Type == PUT && !ignore {
			kv.kvMapper[op.Key] = op.Value
		} else if op.Type == APPEND && !ignore {
			if curVal, ok := kv.kvMapper[op.Key]; ok {
				kv.kvMapper[op.Key] = curVal + op.Value
			} else {
				kv.kvMapper[op.Key] = op.Value
			}
		}

		kv.checkNewSnapshotNeeded()

		indexId := IndexId{}
		indexId.ClientId = clientId
		indexId.RequestId = requestId
		indexId.Index = index

		if ch, ok := kv.indextoChMapper[indexId]; ok {
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
	op.RequestId = args.RequestId
	op.ClientId = args.ClientId

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	indexId := IndexId{}
	indexId.Index = index
	indexId.ClientId = args.ClientId
	indexId.RequestId = args.RequestId

	isApplied := kv.waitForOpConfirmation(indexId)
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
	op.ClientId = args.ClientId
	op.RequestId = args.RequestId
	DPrintf("[Server %d] Received PutAppend for Key %v, value %v", kv.me, op.Key, op.Value)

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	indexId := IndexId{}
	indexId.Index = index
	indexId.ClientId = args.ClientId
	indexId.RequestId = args.RequestId

	isApplied := kv.waitForOpConfirmation(indexId)

	reply.Err = ErrWrongLeader
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
	kv.indextoChMapper = make(map[IndexId]chan raft.ApplyMsg)
	kv.clientsLastRequest = make(map[int64]int64)

	kv.applySnapshot()
	go kv.receiveApply()

	return kv
}
