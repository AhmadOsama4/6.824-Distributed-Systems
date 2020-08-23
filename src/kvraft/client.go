package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu         sync.Mutex
	clientId   int64
	requestId  int64
	lastLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.requestId = 1
	ck.lastLeader = 0
	return ck
}

func (ck *Clerk) GetNewRequestId() int64 {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	val := ck.requestId
	ck.requestId++
	return val
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	args := GetArgs{}
	reply := GetReply{}
	args.Key = key
	args.ClientId = ck.clientId
	args.RequestId = ck.GetNewRequestId()
	DPrintf("[Client %v] Received Get for Key: %v  RequestId %v\n", args.ClientId, key, args.RequestId)

	ret := ""

	ck.mu.Lock()
	lastLeader := ck.lastLeader
	ck.mu.Unlock()

	for i := lastLeader; ; i++ {
		index := i % len(ck.servers)
		DPrintf("[Client %v] Sending Get to server %v reqId %v, Key: %v\n", args.ClientId, index, args.RequestId, key)
		ok := ck.servers[index].Call("KVServer.Get", &args, &reply)

		if ok && reply.Err != ErrWrongLeader {
			if reply.Err == OK {
				ret = reply.Value
				DPrintf("[Client %v] Get result for Key %v = (%v)\n", args.ClientId, args.Key, reply.Value)
			}
			ck.mu.Lock()
			ck.lastLeader = index
			ck.mu.Unlock()
			break
		}
	}

	return ret
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	reply := PutAppendReply{}

	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.clientId
	args.RequestId = ck.GetNewRequestId()

	DPrintf("[Client %v] Received PutAppend reqId %v for Key: %v Value: %v Op: %v\n", args.ClientId, args.RequestId, key, value, op)

	ck.mu.Lock()
	lastLeader := ck.lastLeader
	ck.mu.Unlock()

	for i := lastLeader; ; i++ {
		index := i % len(ck.servers)
		DPrintf("[Client %v] Sending PutAppend to server %v reqId %v, Key: %v, Value %v\n", args.ClientId, index, args.RequestId, key, value)
		ok := ck.servers[index].Call("KVServer.PutAppend", &args, &reply)

		if ok && reply.Err == OK {
			ck.mu.Lock()
			DPrintf("[Client %v] PutAppend completed server %v reqId %v. Key: %v, Value %v\n", args.ClientId, index, args.RequestId, key, value)
			ck.lastLeader = index
			ck.mu.Unlock()
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
