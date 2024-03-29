package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrReqTimedOut = "ErrReqTimedOut"
	ErrNoConfig    = "ErrNoConfig"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	RequestId int64
	ConfigNum int
	ShardNum  int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	RequestId int64
	ConfigNum int
	ShardNum  int
}

type GetReply struct {
	Err   Err
	Value string
}

type GetShardsArgs struct {
	ConfigNum      int
	SendingShardId int // Id of shard requesting data
	Shards         []int
}

type GetShardsReply struct {
	Err       Err
	Data      map[string]KeyMigratingData
	ConfigNum int
}

type KeyMigratingData struct {
	Value             string
	ClientLastRequest map[int64]int64
}
