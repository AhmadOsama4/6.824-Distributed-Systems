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
	GET           = "Get"
	PUT           = "Put"
	APPEND        = "Append"
	CONFIG_CHANGE = "ConfigChange"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key        string
	Value      string
	Type       string
	RequestId  int64
	ClientId   int64
	NewConfing shardmaster.Config
}

type KeyLastRequestInfo struct {
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

	isMigrating bool

	shardAwaitingData  map[int]bool                                // waiting to receive data for this shard
	shardMigratingKeys map[int]map[int]map[string]KeyMigratingData // ConfingNum -> Shard -> keys of the shard leaving the group

	indextoChMapper    map[IndexId]chan Response
	clientsLastRequest map[int64]int64
	kvMapper           map[string]string

	keyLastRequestInfo map[string]KeyLastRequestInfo // last RequestId and ClientId that accessed this key
}

const Debug = 1

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

func getConfigCopy(config shardmaster.Config) shardmaster.Config {
	configCopy := shardmaster.Config{}
	configCopy.Num = config.Num
	copy(configCopy.Shards[:], config.Shards[:shardmaster.NShards])

	configCopy.Groups = make(map[int][]string)
	for k, v := range config.Groups {
		configCopy.Groups[k] = v
	}

	return configCopy
}

func (kv *ShardKV) isLeader() bool {
	return kv.rf.IsLeader()
}

func (kv *ShardKV) isLeavingShard(shardNum int, newConfig shardmaster.Config) bool {
	return newConfig.Shards[shardNum] != kv.gid && kv.config.Shards[shardNum] == kv.gid
}

func (kv *ShardKV) isAwaitingShard(shardNum int, newConfig shardmaster.Config) bool {
	return newConfig.Shards[shardNum] == kv.gid && kv.config.Shards[shardNum] != kv.gid
}

func keyBelongsToMigratingShard(key string, migratingShards []int) (bool, int) {
	keyShard := key2shard(key)
	for _, migratingShard := range migratingShards {
		if keyShard == migratingShard {
			return true, keyShard
		}
	}
	return false, -1
}

func (kv *ShardKV) fetchMissingShardsFromSingleGroup(gid int, shardsToGet []int, configNum int) {
	if len(shardsToGet) == 0 {
		return
	}
	args := GetShardsArgs{}
	args.ConfigNum = configNum
	args.SendingShardId = kv.me
	args.Shards = shardsToGet
	DPrintf("[KV GID %d Server %d] Fetching shards: %v from GID %v\n", kv.gid, kv.me, shardsToGet, gid)

	for {
		kv.mu.Lock()
		servers, ok := kv.config.Groups[gid]
		kv.mu.Unlock()
		if ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply GetShardsReply
				ok := srv.Call("ShardKV.RequestShards", &args, &reply)

				if ok && reply.Err == OK && reply.ConfigNum == configNum {
					kv.mu.Lock()
					DPrintf("[KV GID %d Server %d] Received shards: %v from GID %v\n", kv.gid, kv.me, shardsToGet, gid)
					for k, keyData := range reply.Data {
						kv.kvMapper[k] = keyData.Value
						lastReqId, found := kv.clientsLastRequest[keyData.ClientId]
						if !found || lastReqId < keyData.RequestId {
							kv.clientsLastRequest[keyData.ClientId] = keyData.RequestId
						}
						kv.keyLastRequestInfo[k] = KeyLastRequestInfo{keyData.RequestId, keyData.ClientId}
					}
					for _, shard := range shardsToGet {
						kv.shardAwaitingData[shard] = false
					}

					kv.mu.Unlock()
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) fetchMissingShards(shardsToGet []int) {
	if len(shardsToGet) == 0 {
		return
	}
	DPrintf("[KV GID %d Server %d] Fetching missing shards: %v\n", kv.gid, kv.me, shardsToGet)
	groupShards := make(map[int][]int)
	for _, shard := range shardsToGet {
		groupId := kv.config.Shards[shard]
		if _, found := groupShards[groupId]; !found {
			groupShards[groupId] = make([]int, 0)
		}
		groupShards[groupId] = append(groupShards[groupId], shard)
	}

	for groupId, shards := range groupShards {
		go kv.fetchMissingShardsFromSingleGroup(groupId, shards, kv.config.Num)
	}
}

func (kv *ShardKV) handleConfigChange(newConfig shardmaster.Config) {
	if newConfig.Num <= kv.config.Num || kv.isMigrating {
		return
	}
	DPrintf("[KV GID %d Server %d] Changing Configuration from %d to %d\n", kv.gid, kv.me, kv.config.Num, newConfig.Num)

	// var shardsLeaving []int
	// var shardsToGet []int

	kv.shardMigratingKeys[newConfig.Num] = make(map[int]map[string]KeyMigratingData)
	kv.shardAwaitingData = make(map[int]bool)

	// for shardNum := 0; shardNum < shardmaster.NShards; shardNum++ {
	// 	if kv.isLeavingShard(shardNum, newConfig) {
	// 		shardsLeaving = append(shardsLeaving, shardNum)
	// 		kv.shardMigratingKeys[newConfig.Num][shardNum] = make(map[string]KeyMigratingData)
	// 		DPrintf("Here1")
	// 	}
	// 	if kv.isAwaitingShard(shardNum, newConfig) {
	// 		shardsToGet = append(shardsToGet, shardNum)
	// 		kv.shardAwaitingData[shardNum] = true
	// 		DPrintf("Here2")
	// 	}
	// }

	// for k, v := range kv.kvMapper {
	// 	isKeyLeaving, shard := keyBelongsToMigratingShard(k, shardsLeaving)
	// 	if !isKeyLeaving {
	// 		continue
	// 	}
	// 	DPrintf("Here3")
	// 	keyLastReq := kv.keyLastRequestInfo[k]
	// 	kv.shardMigratingKeys[newConfig.Num][shard][k] = KeyMigratingData{v, keyLastReq.RequestId, keyLastReq.ClientId}
	// 	delete(kv.kvMapper, k)
	// 	delete(kv.keyLastRequestInfo, k)
	// }

	kv.config.Num = newConfig.Num
	DPrintf("[KV GID %d Server %d] New Config: %v\n", kv.gid, kv.me, kv.config.Num)
	copy(kv.config.Shards[:], newConfig.Shards[:shardmaster.NShards])
	kv.config.Groups = make(map[int][]string)
	for k, v := range newConfig.Groups {
		kv.config.Groups[k] = v
	}

	// kv.fetchMissingShards(shardsToGet)
}

// Check if ShardGroup is waiting to receive data for any shard
func (kv *ShardKV) isAwaitingAnyShard() bool {
	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	// for _, v := range kv.shardAwaitingData {
	// 	if v == true {
	// 		return true
	// 	}
	// }
	return false
}

func (kv *ShardKV) detectConfigChange() {
	for {
		time.Sleep(100 * time.Millisecond)
		if !kv.isLeader() {
			continue
		}
		fetchedConfig := kv.sm.Query(-1)
		kv.mu.Lock()
		// DPrintf("Detect Config Acquired lock")
		currentConfigNum := kv.config.Num
		isMigrating := kv.isMigrating
		kv.mu.Unlock()
		// DPrintf("Detect Config Released lock")
		if !isMigrating && fetchedConfig.Num > currentConfigNum && !kv.isAwaitingAnyShard() && kv.isLeader() {
			// kv.handleConfigChange(&fetchedConfig)
			op := Op{}
			op.Type = CONFIG_CHANGE
			op.NewConfing = getConfigCopy(fetchedConfig)

			// If not leader Start will be rejected by Raft
			DPrintf("[KV GID %d Server %d] ConfigChange Calling Start, from %v to %v", kv.gid, kv.me, currentConfigNum, op.NewConfing.Num)
			index, _, isLeader := kv.rf.Start(op)
			DPrintf("[KV GID %d Server %d] ConfigChange Start Finish", kv.gid, kv.me)
			if isLeader {
				DPrintf("[KV GID %d Server %d isLeader] ChangeConfig index %v\n", kv.gid, kv.me, index)
			}
			time.Sleep(100 * time.Millisecond)
		}

	}
}

func (kv *ShardKV) receiveApply() {
	for {
		msg := <-kv.applyCh

		op := msg.Command.(Op)
		msgType := op.Type
		DPrintf("[KV GID %d Server %d] Received apply message op %v\n", kv.gid, kv.me, op)

		index := msg.CommandIndex
		term := msg.CommandTerm
		clientId := op.ClientId
		requestId := op.RequestId

		var newConfig shardmaster.Config

		if msgType == CONFIG_CHANGE {
			newConfig = getConfigCopy(op.NewConfing)
		}

		kv.mu.Lock()
		// defer kv.mu.Unlock()
		kv.lastAppliedIndex = index
		kv.lastAppliedTerm = term

		if msgType == CONFIG_CHANGE {
			kv.handleConfigChange(newConfig)
			kv.mu.Unlock()
			continue
		}
		ignore := false

		if clientLastRequest, found := kv.clientsLastRequest[clientId]; found {
			ignore = clientLastRequest >= requestId
		}

		response := Response{ErrWrongLeader}
		DPrintf("Applying msgType: %v, K: %v, V: %v\n", msgType, op.Key, op.Value)
		if !ignore {
			kv.clientsLastRequest[clientId] = requestId
			kv.keyLastRequestInfo[op.Key] = KeyLastRequestInfo{requestId, clientId}

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
const APPLY_WAIT_MS = 700

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

func (kv *ShardKV) checkRequestError(shardNum int, configNum int) Err {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[KV GID %d Server %d] Checking ShardNum %v ConfigNum %v\n", kv.gid, kv.me, shardNum, configNum)
	DPrintf("[KV GID %d Server %d] Current Config %v Shard belogs to GID %v\n", kv.gid, kv.me, kv.config.Num, kv.gid)
	if configNum != kv.config.Num || kv.gid != kv.config.Shards[shardNum] {
		DPrintf("Rejected 1\n")
		return ErrWrongGroup
	}

	// Not yet received the shard
	if isWaiting, found := kv.shardAwaitingData[shardNum]; found && isWaiting {
		return ErrWrongGroup
	}

	return OK
}

func (kv *ShardKV) RequestShards(args *GetShardsArgs, reply *GetShardsReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shardsMap, found := kv.shardMigratingKeys[args.ConfigNum]
	if !found {
		reply.Err = ErrNoConfig
		return
	}
	reply.ConfigNum = args.ConfigNum
	reply.Err = OK
	reply.Data = make(map[string]KeyMigratingData)

	for _, shardData := range shardsMap {
		for k, v := range shardData {
			reply.Data[k] = v
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[KV GID %d Server %d] Received Get for Key: %v\n", kv.gid, kv.me, args.Key)
	// if err := kv.checkRequestError(args.ShardNum, args.ConfigNum); err != OK {
	// 	reply.Err = err
	// 	return
	// }
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
	DPrintf("[KV GID %d Server %d] Received PutAppend for Key %v, value %v", kv.gid, kv.me, args.Key, args.Value)
	// if err := kv.checkRequestError(args.ShardNum, args.ConfigNum); err != OK {
	// 	reply.Err = err
	// 	return
	// }
	op := Op{}
	op.Key = args.Key
	op.Value = args.Value
	op.Type = args.Op
	op.ClientId = args.ClientId
	op.RequestId = args.RequestId
	op.NewConfing = shardmaster.Config{}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[KV GID %d Server %d] Performing PutAppend Index %v for Key %v, value %v", kv.gid, kv.me, index, args.Key, args.Value)

	indexId := IndexId{}
	indexId.Index = index
	indexId.ClientId = args.ClientId
	indexId.RequestId = args.RequestId

	response := kv.getOpConfirmationWithTimeout(indexId)
	DPrintf("[KV GID %d Server %d] Response %v", kv.gid, kv.me, response)
	reply.Err = ErrWrongLeader
	if response.Err == OK {
		DPrintf("[KV GID %d Server %d] PutAppend Succeeded for K: %v, V: %v\n", kv.gid, kv.me, args.Key, args.Value)
		reply.Err = OK
		return
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
	kv.config = kv.sm.Query(-1)

	kv.isMigrating = false

	kv.kvMapper = make(map[string]string)
	kv.indextoChMapper = make(map[IndexId]chan Response)
	kv.clientsLastRequest = make(map[int64]int64)
	kv.keyLastRequestInfo = make(map[string]KeyLastRequestInfo)
	kv.shardAwaitingData = make(map[int]bool)
	kv.shardMigratingKeys = make(map[int]map[int]map[string]KeyMigratingData)

	go kv.receiveApply()
	go kv.detectConfigChange()

	return kv
}
