package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	LEADER    = "LEADER"
	FOLLOWER  = "FOLLOWER"
	CANDIDATE = "CANDIDATE"
)

const MIN_TIMEOUT = 350
const MAX_TIMEOUT = 550

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//receivedVotes     int
	//receivedResponses int
	currentState string
	currentTerm  int
	votedFor     int
	numPeers     int       // number of peers in the cluster
	timeoutTime  time.Time // time to start a new election if no leader
}

// To be executed when holding the lock
func (rf *Raft) ResetTimeoutTimer() {
	rand.Seed(time.Now().UnixNano())
	rangeRand := rand.Intn(rf.numPeers)
	timeoutDuration := (MAX_TIMEOUT - MIN_TIMEOUT) / rf.numPeers
	timeoutDuration = MIN_TIMEOUT + rangeRand*timeoutDuration
	//timeoutDuration := rand.Intn(MAX_TIMEOUT-MIN_TIMEOUT) + MIN_TIMEOUT

	//rf.timeoutTime = time.Now().Add(time.Millisecond * time.Duration(timeoutDuration))
	rf.timeoutTime = time.Now().Add(time.Millisecond * time.Duration(timeoutDuration))
	DPrintf("[%d] Resetting timeout for Server %d, state: %s, to be after %d ms", rf.currentTerm, rf.me, rf.currentState, timeoutDuration)
}

// Send hearbeat request to follower servers
// should not execute these fn while holding the mu lock
func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	currentIndex := rf.me
	currentState := rf.currentState
	rf.ResetTimeoutTimer()
	rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i == currentIndex {
			continue
		}

		go func(server int) {
			request := &AppendEntriesArgs{}
			reply := &AppendEntriesReply{}

			request.Term = currentTerm
			request.LeaderId = currentIndex

			ok := rf.sendAppendEntries(server, request, reply)
			DPrintf("[%d] HeartBeat Request from %d to %d, sender state: %s, success => %t, receiverTerm %d\n", currentTerm, currentIndex, server, currentState, ok, reply.Term)

			rf.mu.Lock()
			if ok && reply.Term > rf.currentTerm {
				rf.currentState = FOLLOWER
				DPrintf("Server %d returned back as follower\n", rf.me)
			}
			rf.mu.Unlock()

		}(i)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentState = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me

	curTerm := rf.currentTerm
	curIndex := rf.me
	rf.mu.Unlock()

	log.Printf("Server %d is starting election curTerm: %d\n", curIndex, curTerm)

	var voteMu sync.Mutex
	cond := sync.NewCond(&voteMu)

	receivedVotes := 1
	receivedResponses := 0

	// Start Elections
	for i, _ := range rf.peers {
		if i == curIndex {
			continue
		}

		go func(peerIndex int) {
			request := RequestVoteArgs{}
			reply := RequestVoteReply{}

			request.Term = curTerm
			request.CandidateId = curIndex
			request.LastLogIndex = -1
			request.LastLogTerm = -1

			log.Printf("Sever %d sending vote request to %d for term %d\n", curIndex, peerIndex, curTerm)
			ok := rf.sendRequestVote(peerIndex, &request, &reply)
			DPrintf("Vote req from %d to %d, term %d => %t\n", curIndex, peerIndex, curTerm, ok)

			voteMu.Lock()
			if reply.Term == curTerm {
				receivedResponses++
				if reply.VoteGranted {
					receivedVotes++
					DPrintf("Server %d received %d votes\n", curIndex, receivedVotes)
				}
			} else if reply.Term > curTerm {
				rf.mu.Lock()
				DPrintf("Server %d is now follower\n", rf.me)
				rf.currentState = FOLLOWER
				rf.currentTerm = reply.Term
				rf.mu.Unlock()
			} else {
				DPrintf("Something wrong happened while voting")
			}

			cond.Broadcast()
			voteMu.Unlock()
		}(i)
	}

	currentState := CANDIDATE
	majority := rf.numPeers / 2

	// Wait
	voteMu.Lock()

	for currentState == CANDIDATE && receivedVotes <= majority && receivedResponses+1 < rf.numPeers {
		rf.mu.Lock()
		currentState = rf.currentState
		//DPrintf("Server %d state: %s receivedVotes %d receivedResponses %d\n", rf.me, rf.currentState, receivedVotes, receivedResponses)
		rf.mu.Unlock()
		cond.Wait()
	}
	// DPrintf("Server %d state after election: %s\n", curIndex, currentState)

	rf.mu.Lock()

	if rf.currentState != CANDIDATE || rf.currentTerm != curTerm {
		rf.mu.Unlock()
		return
	}

	//DPrintf("Server %d done with election in state %d\n", rf.me, rf.)
	if receivedVotes > majority {
		rf.currentState = LEADER
		DPrintf("Server %d won the elections for term %d\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		go rf.sendHeartbeats()
	} else {
		rf.mu.Unlock()
	}

	voteMu.Unlock()
}

func (rf *Raft) CheckTimeout() {
	for !rf.killed() {
		rf.mu.Lock()
		timeoutVal := rf.timeoutTime
		curState := rf.currentState
		curTerm := rf.currentTerm

		if time.Now().Sub(timeoutVal) <= 0 { // Start new election
			fmt.Println("[", curTerm, "]", "Cur State", curState, "Time now", time.Now(), "timeout time", timeoutVal)
			rf.ResetTimeoutTimer()
			if curState != LEADER {
				go rf.startElection()
			}
		}
		rf.mu.Unlock()

		time.Sleep(time.Millisecond * 300)
	}
}

// Send empty appendEntries message to all followers to maintain leadership
func (rf *Raft) SendEmptyAppendIfLeader() {
	for !rf.killed() {
		rf.mu.Lock()
		isLeader := rf.currentState == LEADER
		rf.mu.Unlock()

		if isLeader {
			rf.sendHeartbeats()
		}

		time.Sleep(time.Millisecond * 150)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int

	//entries []string
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	//rf.mu.Lock()
	log.Printf("Server %d confirms server %d is leader for term %d\n", rf.me, args.LeaderId, args.Term)
	rf.currentState = FOLLOWER
	rf.currentTerm = args.Term
	rf.ResetTimeoutTimer()

	reply.Success = true
	reply.Term = args.Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.currentState == LEADER
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// either voted for self or got the term of another leader
	DPrintf("Server %d received vote req from %d for term %d currentTerm: %d", rf.me, args.CandidateId, args.Term, rf.currentTerm)
	if args.Term <= rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		// TODO: perferm logs check

		reply.VoteGranted = true
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.currentState = FOLLOWER

		rf.votedFor = args.CandidateId
		rf.ResetTimeoutTimer()
	}

	DPrintf("Server %d voted for %d currentTerm %d\n", rf.me, args.CandidateId, rf.currentTerm)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = -1
	rf.numPeers = len(peers)
	rf.currentState = FOLLOWER
	rf.votedFor = -1

	DPrintf("\nNumber of peers: %d\n", rf.numPeers)

	rf.ResetTimeoutTimer()

	go rf.CheckTimeout()
	go rf.SendEmptyAppendIfLeader()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
