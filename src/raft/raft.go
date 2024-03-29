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
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
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
	CommandTerm  int
	IsSnapshot   bool
}

const (
	LEADER    = "LEADER"
	FOLLOWER  = "FOLLOWER"
	CANDIDATE = "CANDIDATE"
)

const MIN_TIMEOUT = 350
const MAX_TIMEOUT = 550

type LogEntry struct {
	LogIndex int
	LogTerm  int
	Command  interface{}
}

type InstallSnapshotRequest struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

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
	currentState      string
	currentTerm       int
	votedFor          int
	numPeers          int       // number of peers in the cluster
	lastHeartbeatTime time.Time // time to start a new election if no leader

	lastAppliedIndex int // index last applied to the service
	commitIndex      int // hightes committed log entries

	lastIncludedIndex int // last index of a snapshot
	lastIncludedTerm  int // last term of a snapshot

	applyCh chan ApplyMsg

	Logs []LogEntry

	nextIndex  map[int]int
	matchIndex map[int]int

	isServerInstallingSnapshot map[int]bool
}

func (rf *Raft) getRandomInterval() int {
	rand.Seed(time.Now().UnixNano())
	rangeRand := rand.Intn(rf.numPeers)
	timeoutDuration := (MAX_TIMEOUT - MIN_TIMEOUT) / rf.numPeers
	timeoutDuration = MIN_TIMEOUT + rangeRand*timeoutDuration

	return timeoutDuration
}

// To be executed while holding the lock
func (rf *Raft) UpdateLastHeartBeat() {
	rf.lastHeartbeatTime = time.Now()
}

// To be executed while holding the lock
func (rf *Raft) getNextLogIndex() int {
	if len(rf.Logs) == 0 {
		if rf.lastIncludedIndex != -1 {
			return rf.lastIncludedIndex + 1
		}
		return 1
	} else {
		return rf.Logs[len(rf.Logs)-1].LogIndex + 1
	}
}

// To be executed while holding the lock
// Return index, term of the last log
func (rf *Raft) getlastLogInfo() (int, int) {
	if len(rf.Logs) == 0 {
		if rf.lastIncludedIndex != -1 {
			return rf.lastIncludedIndex, rf.lastIncludedTerm
		}
		return -1, -1
	} else {
		entry := &rf.Logs[len(rf.Logs)-1]

		return entry.LogIndex, entry.LogTerm
	}
}

// To be executed while holding the lock
// Return index, term of the log at index
func (rf *Raft) getInfoAt(index int) (int, int) {
	if rf.lastIncludedIndex != -1 && index == rf.lastIncludedIndex {
		return rf.lastIncludedIndex, rf.lastIncludedTerm
	}
	if index <= 0 || len(rf.Logs) == 0 {
		DPrintf("[WARN][Raft %d] No info at index %d\n", rf.me, index)
		return -1, -1
	}
	i := index - rf.Logs[0].LogIndex
	if i < 0 || i >= len(rf.Logs) {
		return -1, -1
	}
	return rf.Logs[i].LogIndex, rf.Logs[i].LogTerm
}

// To be executed while holding the lock
func (rf *Raft) getEntryAt(index int) (bool, LogEntry) {
	if index <= 0 {
		return false, LogEntry{}
	}
	if len(rf.Logs) == 0 {
		DPrintf("[WARN] Length of logs is 0, inputIndex %d, lastIncludeIndex %d\n", index, rf.lastIncludedIndex)
		return false, LogEntry{}
	}
	i := index - rf.Logs[0].LogIndex

	if i >= len(rf.Logs) {
		DPrintf("[WARN] index %d > logLength %d\n", i, len(rf.Logs))
		return false, LogEntry{}
	}

	if i < 0 {
		DPrintf("[WARN][Raft %d] curTerm %d index %d LogSize %d firstLogIndex %d\n", rf.me, rf.currentTerm, index, len(rf.Logs), rf.Logs[0].LogIndex)
		return false, LogEntry{}
	}

	return true, rf.Logs[i]
}

// To be executed while holding the lock
func (rf *Raft) getEntriesFromTo(fromIndex int, toIndex int) []LogEntry {
	if toIndex < fromIndex {
		DPrintf("[ERROR] toIndex less than fromIndex %d vs %d\n", toIndex, fromIndex)
		return []LogEntry{}
	}
	// if fromIndex > len(rf.Logs) && {
	// 	DPrintf("[ERROR] fromIndex is larger than logLength\n")
	// 	return []LogEntry{}
	// }
	if len(rf.Logs) == 0 {
		DPrintf("[WARN] Server %d Log length is empty\n", rf.me)
		return []LogEntry{}
	}

	i := fromIndex - rf.Logs[0].LogIndex
	j := i + (toIndex - fromIndex) + 1
	if j > len(rf.Logs) {
		j = len(rf.Logs)
	}

	// Create a copy slice, to avoid passing by refrence
	ret := make([]LogEntry, len(rf.Logs[i:j]))
	copy(ret, rf.Logs[i:j])

	return ret
}

// To be executed while holding the lock
func (rf *Raft) getEntriesFrom(fromIndex int) []LogEntry {
	if fromIndex <= 0 || fromIndex > len(rf.Logs) {
		return []LogEntry{}
	}
	i := fromIndex - rf.Logs[0].LogIndex

	return rf.Logs[i:]
}

// To be executed while holding the lock
// Returns the lower bound log based on term value
func (rf *Raft) getLowerBoundEntry(term int) (bool, LogEntry) {
	retBool := false
	retEntry := LogEntry{}

	L := 0
	R := len(rf.Logs) - 1
	index := -1

	for L <= R {
		var mid int = (L + R) / 2
		if rf.Logs[mid].LogTerm >= term {
			index = mid
			R = mid - 1
		} else {
			L = mid + 1
		}
	}

	if index != -1 {
		retBool = true
		retEntry = rf.Logs[index]
	}

	return retBool, retEntry
}

func (rf *Raft) findTermHighestIndex(term int) (bool, LogEntry) {
	retBool := false
	retEntry := LogEntry{}

	L := 0
	R := len(rf.Logs) - 1
	index := -1

	for L <= R {
		var mid int = (L + R) / 2
		termVal := rf.Logs[mid].LogTerm
		if termVal <= term {
			L = mid + 1
			if termVal == term {
				index = mid
			}
		} else {
			R = mid - 1
		}
	}

	if index != -1 {
		retBool = true
		retEntry = rf.Logs[index]
	}

	return retBool, retEntry
}

func (rf *Raft) getUpperboudIndex(index int) int {
	ret := -1
	L := 0
	R := len(rf.Logs) - 1

	for L <= R {
		var mid int = (L + R) / 2
		curIndex := rf.Logs[mid].LogIndex
		if curIndex > index {
			ret = mid
			R = mid - 1
		} else {
			L = mid + 1
		}
	}

	return ret
}

func (rf *Raft) IsLeader() bool {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentState == LEADER
}

// To be executed while holding the lock
func (rf *Raft) updateTerm(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}
}

// To be executed while holding the lock
func (rf *Raft) discardLogsBeforeIndex(index int) {
	upperIndex := rf.getUpperboudIndex(index)
	if upperIndex == -1 {
		rf.Logs = []LogEntry{}
	} else {
		rf.Logs = rf.Logs[upperIndex:]
	}
	DPrintf("[Raft %d] Logs discarded before index %d, logLength %d\n", rf.me, index, len(rf.Logs))
}

// To be executed while holding lock
func (rf *Raft) GetRaftStateData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.Logs)

	raftState := w.Bytes()
	return raftState
}

func (rf *Raft) SaveSnapshotData(snapshotData []byte, index int, term int) {
	DPrintf("[Raft %d] received snapshot at index %d term %d\n", rf.me, index, term)
	rf.mu.Lock()

	if index <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	rf.discardLogsBeforeIndex(index)

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term
	// rf.lastAppliedIndex = index
	//rf.currentTerm = term
	raftState := rf.GetRaftStateData()

	rf.mu.Unlock()
	rf.persister.SaveStateAndSnapshot(raftState, snapshotData)
}

func (rf *Raft) GetSnapshotData() []byte {
	return rf.persister.ReadSnapshot()
}

// Send hearbeat request to follower servers
// should not execute these fn while holding the mu lock
func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	currentIndex := rf.me
	//currentState := rf.currentState
	currentCommit := rf.commitIndex
	lastIndex, lastTerm := rf.getlastLogInfo()
	rf.UpdateLastHeartBeat()
	rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i == currentIndex {
			continue
		}

		go func(server int) {
			request := AppendEntriesArgs{}
			reply := AppendEntriesReply{}

			request.Term = currentTerm
			request.LeaderId = currentIndex
			request.LeaderCommit = currentCommit
			request.PrevLogIndex = lastIndex
			request.PrevLogTerm = lastTerm

			ok := rf.sendAppendEntries(server, &request, &reply)
			//DPrintf("[%d] HeartBeat Request from %d to %d, sender state: %s, success => %t, receiverTerm %d\n", currentTerm, currentIndex, server, currentState, ok, reply.Term)

			rf.mu.Lock()
			if ok && reply.Term > rf.currentTerm {
				rf.currentState = FOLLOWER
				DPrintf("Server %d returned back as follower\n", rf.me)
				rf.mu.Unlock()
				return
			}

			// follower out of sync
			if ok && !reply.Success {
				request.LeaderCommit = rf.commitIndex
				request.LeaderId = rf.me
				request.Term = rf.currentTerm

				go rf.handleServerAppendLogs(server)
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

	rf.persist()

	curTerm := rf.currentTerm
	curIndex := rf.me
	lastLogIndex, lastLogTerm := rf.getlastLogInfo()
	rf.mu.Unlock()

	DPrintf("Server %d is starting election curTerm: %d\n", curIndex, curTerm)

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
			request.LastLogIndex = lastLogIndex
			request.LastLogTerm = lastLogTerm

			DPrintf("Sever %d sending vote request to %d for term %d\n", curIndex, peerIndex, curTerm)
			ok := rf.sendRequestVote(peerIndex, &request, &reply)
			//DPrintf("Vote req from %d to %d, term %d => %t\n", curIndex, peerIndex, curTerm, ok)

			voteMu.Lock()
			receivedResponses++
			if ok {
				if reply.Term == curTerm {
					if reply.VoteGranted {
						receivedVotes++
						DPrintf("Server %d received %d votes\n", curIndex, receivedVotes)
					}
				} else if reply.Term > curTerm {
					rf.mu.Lock()
					DPrintf("Server %d is now follower\n", rf.me)
					rf.currentState = FOLLOWER
					rf.updateTerm(reply.Term)
					//rf.currentTerm = reply.Term
					rf.persist()
					rf.mu.Unlock()
				} else {
					DPrintf("Something wrong happened while voting")
				}

			} else {
				DPrintf("[%d] Error sending vote from %d to %d\n", curTerm, rf.me, peerIndex)
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

	DPrintf("Server %d done with election in state %v\n", rf.me, rf.currentState)
	if receivedVotes > majority {
		rf.becomeLeader()
		rf.mu.Unlock()
	} else {
		DPrintf("[%d] Server %d has not received majority\n", rf.currentTerm, rf.me)
		rf.mu.Unlock()
	}

	voteMu.Unlock()
}

// to be executed while holding the lock
func (rf *Raft) becomeLeader() {
	rf.currentState = LEADER
	initNextIndex := rf.getNextLogIndex()

	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}

		rf.isServerInstallingSnapshot[server] = false
		rf.nextIndex[server] = initNextIndex
		rf.matchIndex[server] = 0
	}

	go rf.sendHeartbeats()
	DPrintf("Server %d won the elections for term %d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) handleServerInstallSnapshot(server int) {
	request := &InstallSnapshotRequest{}
	reply := &InstallSnapshotReply{}
	DPrintf("Installing Snapshot for server %d\n", server)
	rf.mu.Lock()
	request.Term = rf.currentTerm
	request.LeaderId = rf.me
	request.LastIncludedIndex = rf.lastIncludedIndex
	request.LastIncludedTerm = rf.lastIncludedTerm
	request.Data = rf.persister.ReadSnapshot()
	rf.mu.Unlock()

	ok := rf.sendInstallSnapshot(server, request, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok {
		DPrintf("[Raft %d] Error sending installSnapshot request to server %d\n", rf.me, server)
		rf.isServerInstallingSnapshot[server] = false
		return
	}
	if reply.Term > rf.currentTerm {
		DPrintf("[Raft %d] Snapshot Sender becoming follower\n", rf.me)
		rf.currentState = FOLLOWER
		rf.updateTerm(reply.Term)
		rf.persist()
		rf.isServerInstallingSnapshot[server] = false
		//rf.mu.Unlock()
		return
	}
	DPrintf("[Raft %d] Snapshot installed on server %d\n", rf.me, server)
	rf.nextIndex[server] = rf.lastIncludedIndex + 1
	rf.matchIndex[server] = rf.lastIncludedIndex
	rf.isServerInstallingSnapshot[server] = false
	go rf.handleServerAppendLogs(server)
	// rf.mu.Unlock()
}

func (rf *Raft) handleServerAppendLogs(server int) {
	request := &AppendEntriesArgs{}
	reply := &AppendEntriesReply{}

	rf.mu.Lock()
	nextIndex := rf.nextIndex[server]

	if isInstalling, found := rf.isServerInstallingSnapshot[server]; found && isInstalling {
		rf.mu.Unlock()
		return
	}

	DPrintf("[Raft %d] Server %d NextIndex %d VS last includedIndex %d\n", rf.me, server, nextIndex, rf.lastIncludedIndex)
	if rf.lastIncludedIndex != -1 && nextIndex <= rf.lastIncludedIndex {
		DPrintf("[Raft %d] Installing Snapshot\n", rf.me)
		rf.isServerInstallingSnapshot[server] = true
		go rf.handleServerInstallSnapshot(server)
		rf.mu.Unlock()
		return
	}
	lastIndex, lastTerm := rf.getlastLogInfo()

	request.LeaderCommit = rf.commitIndex
	request.LeaderId = rf.me
	request.Term = rf.currentTerm
	request.PrevLogIndex, request.PrevLogTerm = rf.getInfoAt(nextIndex - 1)

	if lastTerm != rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	DPrintf("Me #%d lastIndex %d lastIncIndex %d, server %d nextIndex %d logsize %d\n", rf.me, lastIndex, rf.lastIncludedIndex, server, nextIndex, len(rf.Logs))
	request.Entries = rf.getEntriesFromTo(nextIndex, lastIndex)
	rf.mu.Unlock()
	DPrintf("[%d] Sending Append Request to server %d\n", request.Term, server)
	DPrintf("Request Details => Length: %d\n", len(request.Entries))

	ok := rf.sendAppendEntries(server, request, reply)

	if !ok {
		return
	}

	rf.mu.Lock()

	if reply.Term > rf.currentTerm {
		rf.currentState = FOLLOWER
		rf.updateTerm(reply.Term)
		rf.persist()
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		if lastIndex > rf.matchIndex[server] {
			rf.nextIndex[server] = lastIndex + 1
			rf.matchIndex[server] = lastIndex
		}
	} else {
		if reply.XTerm == -1 {
			rf.nextIndex[server] = reply.XLen + 1
		} else {
			found, entry := rf.findTermHighestIndex(reply.XTerm)
			if found {
				rf.nextIndex[server] = entry.LogIndex + 1
			} else {
				rf.nextIndex[server] = reply.XIndex
			}
		}
	}

	continueSending := rf.nextIndex[server] <= lastIndex

	rf.mu.Unlock()

	if !reply.Success && continueSending {
		rf.handleServerAppendLogs(server)
	}
}

// Replicated Logs on leader and followers
func (rf *Raft) ReplicateLogs(index int, command interface{}) {
	//rf.mu.Lock()

	DPrintf("[%d] Replicating Logs, Leader: %d, index: %d\n", rf.currentTerm, rf.me, index)

	entry := LogEntry{}
	entry.Command = command
	entry.LogIndex = index
	entry.LogTerm = rf.currentTerm

	rf.Logs = append(rf.Logs, entry)
	rf.persist()
	DPrintf("Log size for leader server %d = %d\n", rf.me, len(rf.Logs))

	//rf.mu.Unlock()

	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}

		go rf.handleServerAppendLogs(server)
	}

}

// Thread for sending confirmations to the Service/Tester
func (rf *Raft) SendConfirmationsThread() {
	for !rf.killed() {
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.lastAppliedIndex >= rf.commitIndex {
				return
			}

			if rf.lastAppliedIndex < rf.lastIncludedIndex {
				apply := ApplyMsg{}
				apply.IsSnapshot = true
				DPrintf("Raft Server %d sending snapshot to KV Server\n", rf.me)
				rf.mu.Unlock()
				rf.applyCh <- apply

				rf.mu.Lock()
				rf.lastAppliedIndex = rf.lastIncludedIndex
				return
			}

			//log.Printf("Last Log Index %d\n", rf.Logs[len(rf.Logs)-1].LogIndex)
			for i := rf.lastAppliedIndex + 1; i <= rf.commitIndex; i++ {
				//log.Printf("(%d) vs (%d) server %d\n", i, len(rf.Logs), rf.me)
				DPrintf("[Raft %d] i: %d, logLen: %d, lastIncIndex: %d, commitIndex: %d\n", rf.me, i, len(rf.Logs), rf.lastIncludedIndex, rf.commitIndex)
				apply := ApplyMsg{}
				apply.CommandValid = true
				apply.CommandIndex = i
				_, entry := rf.getEntryAt(i)
				apply.CommandTerm = entry.LogTerm
				apply.Command = entry.Command
				apply.IsSnapshot = false

				DPrintf("[Raft %d] sending confirmation to Service lastIncIndx %d LogSize %d, index: %d command %v\n", rf.me, rf.lastIncludedIndex, len(rf.Logs), i, apply.Command)
				rf.applyCh <- apply

				rf.lastAppliedIndex = i
			}
		}()

		time.Sleep(10 * time.Millisecond)
	}
}

// Thread for updating the last committed index for Leader server
func (rf *Raft) UpdateLastCommitIndex() {
	for !rf.killed() {
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.currentState != LEADER {
				return
			}

			lastIndex, _ := rf.getlastLogInfo()

			for i := rf.commitIndex + 1; i <= lastIndex; i++ {
				replicatedCount := 1

				for server, _ := range rf.peers {
					if rf.matchIndex[server] >= i {
						replicatedCount++
					}
				}

				found, entry := rf.getEntryAt(i)
				if !found {
					DPrintf("[ERROR][Raft %d] Entry not found while updating LastCommitIndex, index %d lastincludeindex %d", rf.me, i, rf.lastIncludedIndex)
				}
				if replicatedCount > rf.numPeers/2 {
					if entry.LogTerm == rf.currentTerm {
						rf.commitIndex = i
						DPrintf("[%d] Server %d, Index %d is replicated on majority of servers\n", rf.currentTerm, rf.me, i)
					}
				} else {
					break
				}
			}

		}()

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) CheckTimeout() {
	for !rf.killed() {
		interval := rf.getRandomInterval()
		timeMS := time.Millisecond * time.Duration(interval)
		time.Sleep(timeMS)

		rf.mu.Lock()

		curState := rf.currentState
		// curTerm := rf.currentTerm

		if time.Now().Sub(rf.lastHeartbeatTime) >= timeMS { // Start new election
			// fmt.Println("[", curTerm, "]", "Cur State", curState, "Time now", time.Now(), "timeout time", timeMS)
			rf.UpdateLastHeartBeat()
			if curState != LEADER {
				go rf.startElection()
			}
		}
		rf.mu.Unlock()

		//time.Sleep(time.Millisecond * 300)
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

	Entries []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	XTerm   int
	XIndex  int
	XLen    int
	Success bool
}

func (rf *Raft) DiscardRepeatedEntries(entries []LogEntry, index int) []LogEntry {
	for i, entry := range entries {
		if entry.LogIndex > index {
			return entries[i:]
		}
	}
	return []LogEntry{}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.currentState = FOLLOWER
	rf.updateTerm(args.Term)
	//rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	reply.Success = false

	//lastIndex, _ := rf.getlastLogInfo()
	lastIndex, lastTerm := rf.getlastLogInfo()
	found, entry := rf.getEntryAt(args.PrevLogIndex)
	if !found {
		DPrintf("Entry not found while appending , args logLength %d\n", len(args.Entries))
	}
	rf.UpdateLastHeartBeat()

	DPrintf("[Raft %d] Received append request from %d lastIndx %d last Term %d VS Sender: lastIndex %d lstTerm %d\n", rf.me, args.LeaderId, lastIndex, lastTerm, args.PrevLogIndex, args.PrevLogTerm)
	var acceptAppend bool = (lastIndex == -1) ||
		args.PrevLogIndex <= 0 ||
		(found && entry.LogIndex == args.PrevLogIndex && entry.LogTerm == args.PrevLogTerm) ||
		(!found && rf.lastIncludedIndex == args.PrevLogIndex && rf.lastIncludedTerm == args.PrevLogTerm)

	if acceptAppend {
		DPrintf("Server %d confirms server %d is leader for term %d\n", rf.me, args.LeaderId, args.Term)
		reply.Success = true
		// Update latest committed index
		rf.commitIndex = args.LeaderCommit
		if args.LeaderCommit > lastIndex {
			rf.commitIndex = lastIndex
		}

		args.Entries = rf.DiscardRepeatedEntries(args.Entries, rf.lastIncludedIndex)

		// Regular Hearbeat message
		if len(args.Entries) == 0 {
			DPrintf("[%d] Server %d received Heartbeat message from %d\n", rf.currentTerm, rf.me, args.LeaderId)
			rf.persist()
			return
		}
		// server has no logs yet
		if lastIndex == -1 || (args.PrevLogIndex == rf.lastIncludedIndex && len(rf.Logs) == 0) {
			rf.Logs = append(rf.Logs, args.Entries...)
		} else {

			i := 0
			// if entry exists at args.PrevLogIndex
			if found {
				i = entry.LogIndex - rf.Logs[0].LogIndex + 1
			}
			lastArgIndex := len(args.Entries) - 1
			lastArgEntry := args.Entries[lastArgIndex]

			if lastArgEntry.LogIndex > lastIndex || lastArgEntry.LogTerm > lastTerm {
				rf.Logs = append(rf.Logs[:i], args.Entries...)
			}
		}

		DPrintf("[%d] Server %d Log Size = %d\n", rf.currentTerm, rf.me, len(rf.Logs))
		lastIndex, _ = rf.getlastLogInfo()
		rf.commitIndex = args.LeaderCommit
		if args.LeaderCommit > lastIndex {
			rf.commitIndex = lastIndex
		}
		// DPrintf("[%d] Appending Done for server %d index %d\n", rf.currentTerm, rf.me, lastIndex)
	} else {
		//reply.Success = false
		if found {
			DPrintf("[%d] Server %d rejected server %d append, CurLogIndex %d curLogTerm %d VS recIndex %d recTerm %d", rf.currentTerm, rf.me, args.LeaderId, entry.LogIndex, entry.LogTerm, args.PrevLogIndex, args.PrevLogTerm)
		} else {
			DPrintf("[%d] Server %d rejected server %d append, LastIncIndex %d LastIncTerm %d VS recIndex %d recTerm %d", rf.currentTerm, rf.me, args.LeaderId, rf.lastIncludedIndex, rf.lastIncludedTerm, args.PrevLogIndex, args.PrevLogTerm)
		}

		reply.XLen = len(rf.Logs)
		if rf.lastIncludedIndex != -1 {
			reply.XLen = len(rf.Logs) + rf.lastIncludedIndex
		}

		if !found {
			reply.XTerm = -1
			reply.XIndex = -1
		} else {
			_, entry := rf.getEntryAt(args.PrevLogIndex)
			reply.XTerm = entry.LogTerm

			_, entry = rf.getLowerBoundEntry(entry.LogTerm)
			reply.XIndex = entry.LogIndex
		}
	}

	rf.persist()

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotRequest, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	DPrintf("[Raft %d] Received Install Snapshot index %d\n", rf.me, args.LastIncludedIndex)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	rf.UpdateLastHeartBeat()
	rf.updateTerm(args.Term)
	reply.Term = rf.currentTerm

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.discardLogsBeforeIndex(rf.lastIncludedIndex)

	rf.commitIndex = rf.lastIncludedIndex

	raftState := rf.GetRaftStateData()
	rf.mu.Unlock()
	rf.persister.SaveStateAndSnapshot(raftState, args.Data)
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.Logs)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int
	var votedFor int
	var lastIndex int
	var lastTerm int
	var logs []LogEntry

	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIndex) != nil ||
		d.Decode(&lastTerm) != nil ||
		d.Decode(&logs) != nil {

		log.Fatal("Cannot Decode Persistent State\n")
	} else {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.lastIncludedIndex = lastIndex
		rf.lastIncludedTerm = lastTerm
		rf.Logs = logs
		rf.mu.Unlock()
	}
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
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
	DPrintf("[Raft %d]Server received vote req from %d for term %d currentTerm: %d", rf.me, args.CandidateId, args.Term, rf.currentTerm)
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// if args.Term > rf.currentTerm {
	// 	rf.votedFor = -1
	// 	rf.currentTerm = args.Term
	// }
	rf.updateTerm(args.Term)

	lastLogIndex, lastLogTerm := rf.getlastLogInfo()
	DPrintf("[Raft %d] Vote req: from %d, lastindex %d, lastterm %d VS me: index %d, lastTerm %d\n", rf.me, args.CandidateId, args.LastLogIndex, args.LastLogTerm, lastLogIndex, lastLogTerm)

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		DPrintf("[Raft %d] Vote Not granted yet votedFor: %d Term: %d\n", rf.me, rf.votedFor, rf.currentTerm)
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			DPrintf("[Raft %d] Vote granted to %d votedFor: %d currentTerm: %d RequestTerm: %d\n", rf.me, args.CandidateId, rf.votedFor, rf.currentTerm, args.Term)
			reply.VoteGranted = true
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			rf.currentState = FOLLOWER

			rf.votedFor = args.CandidateId

			rf.UpdateLastHeartBeat()
			DPrintf("Server %d voted for %d currentTerm %d\n", rf.me, args.CandidateId, rf.currentTerm)
		}
	}
	rf.persist()

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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotRequest, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.currentState == LEADER

	if isLeader {
		index = rf.getNextLogIndex()
		DPrintf("[%d] Server %d Received Start Req Command %v Index %d\n", rf.currentTerm, rf.me, command, index)
		term = rf.currentTerm
		rf.ReplicateLogs(index, command)
	}

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
	rf.currentTerm = 0
	rf.numPeers = len(peers)
	rf.currentState = FOLLOWER
	rf.votedFor = -1
	rf.lastAppliedIndex = 0
	rf.commitIndex = 0

	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = -1

	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)

	rf.isServerInstallingSnapshot = make(map[int]bool)

	rf.applyCh = applyCh

	DPrintf("\nNumber of peers: %d\n", rf.numPeers)

	rf.UpdateLastHeartBeat()

	go rf.CheckTimeout()
	go rf.SendEmptyAppendIfLeader()
	go rf.UpdateLastCommitIndex()
	go rf.SendConfirmationsThread()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
