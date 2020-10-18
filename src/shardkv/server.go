package shardkv

// import "../shardmaster"
import (
	"log"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastAppliedIndex int
	lastAppliedTerm  int

	sm     *shardmaster.Clerk
	config shardmaster.Config

	indextoChMapper    map[IndexId]chan Response
	clientsLastRequest map[int64]int64
	kvMapper           map[string]string
}

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type IndexId struct {
	Index     int
	RequestId int64
	ClientId  int64
}

type Response struct {
	Err Err
}

func (kv *ShardKV) handleConfigChange(newConfig *shardmaster.Config) {
	DPrintf("[KV GID %d Server %d] Changing Configuration from %d to %d\n", kv.gid, kv.me, kv.config.Num, newConfig.Num)
	kv.config.Num = newConfig.Num
	copy(kv.config.Shards[:], newConfig.Shards[:shardmaster.NShards])
	kv.config.Groups = make(map[int][]string)
	DPrintf("New Shards: %v\n", kv.config.Shards)
	for k, v := range newConfig.Groups {
		kv.config.Groups[k] = v
	}
}

func (kv *ShardKV) detectConfigChange() {
	for {
		fetchedConfig := kv.sm.Query(-1)
		kv.mu.Lock()
		if fetchedConfig.Num > kv.config.Num {
			kv.handleConfigChange(&fetchedConfig)
		}
		kv.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) receiveApply() {
	for {
		msg := <-kv.applyCh

		op := msg.Command.(Op)
		index := msg.CommandIndex
		term := msg.CommandTerm

		clientId := op.ClientId
		requestId := op.RequestId
		msgType := op.Type
		DPrintf("[KV GID %d Server %d] Received apply message of type %v\n", kv.gid, kv.me, msgType)

		kv.mu.Lock()
		kv.lastAppliedIndex = index
		kv.lastAppliedTerm = term

		ignore := false

		if clientLastRequest, found := kv.clientsLastRequest[clientId]; found {
			ignore = clientLastRequest >= requestId
		}

		response := Response{ErrWrongLeader}

		if !ignore {
			kv.clientsLastRequest[clientId] = requestId
			if msgType == GET {
				response = Response{OK}
			} else if msgType == PUT {
				kv.kvMapper[op.Key] = op.Value
				response = Response{OK}
			} else if msgType == APPEND {
				if curVal, ok := kv.kvMapper[op.Key]; ok {
					kv.kvMapper[op.Key] = curVal + op.Value
				} else {
					kv.kvMapper[op.Key] = op.Value
				}
				response = Response{OK}
				DPrintf("")
			}
		}

		indexId := IndexId{index, requestId, clientId}
		if ch, ok := kv.indextoChMapper[indexId]; ok {
			ch <- response
		}

		kv.mu.Unlock()
	}
}

// Time to wait for receiving confirmation from Raft peer
const APPLY_WAIT_MS = 500

func (kv *ShardKV) getOpConfirmationWithTimeout(indexId IndexId) Response {
	ch := make(chan Response, 1)

	kv.mu.Lock()
	kv.indextoChMapper[indexId] = ch
	kv.mu.Unlock()

	ret := Response{ErrReqTimedOut}

	select {
	case ret = <-ch:
	case <-time.After(APPLY_WAIT_MS * time.Millisecond):
	}
	kv.mu.Lock()
	delete(kv.indextoChMapper, indexId)
	kv.mu.Unlock()

	return ret
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[KV GID %d Server %d] Received Get for Key: %v\n", kv.gid, kv.me, args.Key)
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

	response := kv.getOpConfirmationWithTimeout(indexId)
	ret := ""
	reply.Err = ErrWrongLeader

	if response.Err != OK {
		return
	}

	DPrintf("[KV GID %d Server %d] Getting Value for Key: %v\n", kv.gid, kv.me, args.Key)

	kv.mu.Lock()
	ret, ok := kv.kvMapper[args.Key]
	DPrintf("[KV GID %d Server %d] Ok %v Key %v Value %v", kv.gid, kv.me, ok, args.Key, ret)
	kv.mu.Unlock()

	if ok {
		reply.Err = OK
		reply.Value = ret
		DPrintf("[KV GID %d Server %d] Key %v found value %v\n", kv.gid, kv.me, args.Key, ret)
	} else {
		reply.Err = ErrNoKey
		DPrintf("[KV GID %d Server %d] Key %v no value found\n", kv.gid, kv.me, args.Key)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{}
	op.Key = args.Key
	op.Value = args.Value
	op.Type = args.Op
	op.ClientId = args.ClientId
	op.RequestId = args.RequestId
	DPrintf("[KV GID %d Server %d] Received PutAppend for Key %v, value %v", kv.gid, kv.me, op.Key, op.Value)

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	indexId := IndexId{}
	indexId.Index = index
	indexId.ClientId = args.ClientId
	indexId.RequestId = args.RequestId

	response := kv.getOpConfirmationWithTimeout(indexId)

	reply.Err = ErrWrongLeader
	if response.Err == OK {
		DPrintf("[KV GID %d Server %d] PutAppend Succeeded for K: %v, V: %v\n", kv.gid, kv.me, args.Key, args.Value)
		reply.Err = OK
	}
	DPrintf("[KV GID %d Server %d] PutAppend Failed for K: %v, V: %v\n", kv.gid, kv.me, args.Key, args.Value)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.sm = shardmaster.MakeClerk(masters)

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.config = shardmaster.Config{}
	kv.config.Num = 0

	kv.kvMapper = make(map[string]string)
	kv.indextoChMapper = make(map[IndexId]chan Response)
	kv.clientsLastRequest = make(map[int64]int64)

	go kv.receiveApply()
	go kv.detectConfigChange()

	return kv
}
