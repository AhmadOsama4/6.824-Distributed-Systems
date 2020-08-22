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
	mu        sync.Mutex
	clientId  int64
	requestId int64
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
	ck.requestId = 0
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
	DPrintf("[Client] Received Get for Key: %v\n", key)

	args := GetArgs{}
	reply := GetReply{}
	args.Key = key
	args.ClientId = ck.clientId
	args.RequestId = ck.GetNewRequestId()

	found := false
	ret := ""
	for !found {
		for i, _ := range ck.servers {
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)

			if ok && reply.Err == OK {
				found = true
				ret = reply.Value
				DPrintf("[Client] Get result for Key %v = (%v)\n", args.Key, reply.Value)
				break
			}
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
	DPrintf("[Client] Received PutAppend for Key: %v Value: %v Op: %v\n", key, value, op)
	args := PutAppendArgs{}
	reply := PutAppendReply{}

	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.clientId
	args.RequestId = ck.GetNewRequestId()

	found := false

	for !found {
		for i, _ := range ck.servers {
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)

			if ok && reply.Err == OK {
				found = true
				break
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
