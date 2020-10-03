package shardmaster

import (
	"log"
	"sort"
	"sync"
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
	JOIN  = "Join"
	LEAVE = "Leave"
	MOVE  = "Move"
	QUERY = "Query"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	lastAppliedIndex int
	lastAppliedTerm  int

	configs []Config // indexed by config num

	indextoChMapper    map[IndexId]chan Response
	clientsLastRequest map[int64]int64
}

type IndexId struct {
	Index     int
	RequestId int64
	ClientId  int64
}

type Response struct {
	Err   string
	Value interface{}
}

// Time to wait for receiving confirmation from Raft peer
const APPLY_WAIT_MS = 500

func (sm *ShardMaster) getOpConfirmationWithTimeout(indexId IndexId) Response {
	ch := make(chan Response, 1)

	sm.mu.Lock()
	sm.indextoChMapper[indexId] = ch
	sm.mu.Unlock()

	ret := Response{ErrReqTimedOut, nil}

	select {
	case ret = <-ch:
	case <-time.After(APPLY_WAIT_MS * time.Millisecond):
	}

	sm.mu.Lock()
	delete(sm.indextoChMapper, indexId)
	sm.mu.Unlock()

	return ret
}

type Op struct {
	// Your data here.
	Type      string
	RequestId int64
	ClientId  int64

	Servers   map[int][]string // Join Arg
	GIDs      []int            // Leave Arg
	Shard     int              // Move Arg
	GID       int              // Move Arg
	ConfigNum int              // Query Arg
}

func (sm *ShardMaster) rebalanceShards(config *Config) {
	numGroups := len(config.Groups)

	gids := []int{}
	for gid, _ := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	avgPerGroup := NShards / numGroups
	limit := avgPerGroup * NShards
	for i := 0; i < limit; i++ {
		groupId := gids[int(NShards/avgPerGroup)]
		config.Shards[i] = groupId
	}

	g := 0
	for i := avgPerGroup * NShards; i < NShards; i++ {
		config.Shards[i] = gids[g]
		g++
	}

}

func (sm *ShardMaster) performJoin(servers map[int][]string) Response {
	newConfig := Config{}

	newGroups := make(map[int][]string)
	for k, v := range sm.configs[len(sm.configs)-1].Groups {
		newGroups[k] = v
	}
	for k, v := range servers {
		newGroups[k] = v
	}

	newConfig.Num = len(sm.configs)
	newConfig.Groups = newGroups
	newConfig.Shards = [NShards]int{}

	sm.rebalanceShards(&newConfig)

	sm.configs = append(sm.configs, newConfig)

	return Response{OK, nil}
}

func (sm *ShardMaster) performLeave(gids []int) Response {
	newConfig := Config{}

	newGroups := make(map[int][]string)
	for k, v := range sm.configs[len(sm.configs)-1].Groups {
		newGroups[k] = v
	}
	for _, gid := range gids {
		delete(newGroups, gid)
	}

	newConfig.Num = len(sm.configs)
	newConfig.Groups = newGroups
	newConfig.Shards = [NShards]int{}

	sm.rebalanceShards(&newConfig)

	sm.configs = append(sm.configs, newConfig)

	return Response{OK, nil}
}

func (sm *ShardMaster) performMove(gid int, shard int) Response {
	newConfig := Config{}

	newGroups := make(map[int][]string)
	for k, v := range sm.configs[len(sm.configs)-1].Groups {
		newGroups[k] = v
	}

	newConfig.Num = len(sm.configs)
	newConfig.Groups = newGroups
	newConfig.Shards = [NShards]int{}
	for i, shardLoc := range sm.configs[len(sm.configs)-1].Shards {
		newConfig.Shards[i] = shardLoc
	}
	newConfig.Shards[shard] = gid

	sm.configs = append(sm.configs, newConfig)

	return Response{OK, nil}
}

func (sm *ShardMaster) performQuery(num int) Response {
	if num < 0 || num >= len(sm.configs) {
		return Response{OK, sm.configs[len(sm.configs)-1]}
	}
	return Response{OK, sm.configs[num]}
}

func (sm *ShardMaster) receiveApply() {
	for {
		msg := <-sm.applyCh

		op := msg.Command.(Op)
		index := msg.CommandIndex
		term := msg.CommandTerm

		clientId := op.ClientId
		requestId := op.RequestId
		msgType := op.Type
		DPrintf("[SM Server %d] Received apply message of type %v\n", sm.me, msgType)

		sm.mu.Lock()
		sm.lastAppliedIndex = index
		sm.lastAppliedTerm = term

		ignore := false

		if clientLastRequest, found := sm.clientsLastRequest[clientId]; found {
			ignore = clientLastRequest >= requestId
		}

		if !ignore {
			sm.clientsLastRequest[clientId] = requestId
		}

		response := Response{ErrWrongLeader, nil}

		if msgType == JOIN {
			response = sm.performJoin(op.Servers)
		} else if msgType == LEAVE {
			response = sm.performLeave(op.GIDs)
		} else if msgType == MOVE {
			response = sm.performMove(op.GID, op.Shard)
		} else if msgType == QUERY {
			response = sm.performQuery(op.ConfigNum)
		}

		indexId := IndexId{index, clientId, requestId}
		if ch, ok := sm.indextoChMapper[indexId]; ok {
			ch <- response
		}

		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{}
	op.Type = JOIN
	op.RequestId = args.RequestId
	op.ClientId = args.ClientId
	op.Servers = args.Servers

	index, _, isLeader := sm.rf.Start(op)

	reply.Err = ErrWrongLeader
	reply.WrongLeader = true

	if !isLeader {
		return
	}

	indexId := IndexId{index, args.ClientId, args.RequestId}

	response := sm.getOpConfirmationWithTimeout(indexId)

	if response.Err != OK {
		return
	}

	reply.Err = OK
	reply.WrongLeader = false

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{}
	op.Type = LEAVE
	op.RequestId = args.RequestId
	op.ClientId = args.ClientId
	op.GIDs = args.GIDs

	index, _, isLeader := sm.rf.Start(op)

	reply.Err = ErrWrongLeader
	reply.WrongLeader = true

	if !isLeader {
		return
	}

	indexId := IndexId{index, args.ClientId, args.RequestId}

	response := sm.getOpConfirmationWithTimeout(indexId)

	if response.Err != OK {
		return
	}

	reply.Err = OK
	reply.WrongLeader = false

}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{}
	op.Type = MOVE
	op.RequestId = args.RequestId
	op.ClientId = args.ClientId
	op.GID = args.GID
	op.Shard = args.Shard

	index, _, isLeader := sm.rf.Start(op)

	reply.Err = ErrWrongLeader
	reply.WrongLeader = true

	if !isLeader {
		return
	}

	indexId := IndexId{index, args.ClientId, args.RequestId}

	response := sm.getOpConfirmationWithTimeout(indexId)

	if response.Err != OK {
		return
	}

	reply.Err = OK
	reply.WrongLeader = false
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{}
	op.Type = MOVE
	op.RequestId = args.RequestId
	op.ClientId = args.ClientId
	op.ConfigNum = args.Num

	index, _, isLeader := sm.rf.Start(op)

	reply.Err = ErrWrongLeader
	reply.WrongLeader = true

	if !isLeader {
		return
	}

	indexId := IndexId{index, args.ClientId, args.RequestId}

	response := sm.getOpConfirmationWithTimeout(indexId)

	if response.Err != OK {
		return
	}

	reply.Err = OK
	reply.WrongLeader = false
	sm.mu.Lock()
	reply.Config = sm.configs[args.Num]
	sm.mu.Unlock()
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.indextoChMapper = make(map[IndexId]chan Response)
	sm.clientsLastRequest = make(map[int64]int64)

	sm.configs = []Config{Config{}}
	sm.configs[0].Num = 0
	sm.configs[0].Shards = [NShards]int{}
	sm.configs[0].Groups = make(map[int][]string)

	go sm.receiveApply()

	return sm
}
