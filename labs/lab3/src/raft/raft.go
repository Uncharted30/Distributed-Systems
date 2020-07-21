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
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
import "../labgob"

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

// A Go object implementing a log entry
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// append entries

// states of server
const (
	follower  = 1
	candidate = 2
	leader    = 3
)

// timeouts
const (
	minTimeout = 350
	maxTimeout = 500
	heartbeat  = 100
	applyTimeout = 100
)

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

	currentTerm          int
	votedFor             int
	log                  []LogEntry
	commitIndex          int
	lastApplied          int
	nextIndex            []int
	matchIndex           []int
	state                int
	timer                *time.Timer
	applyCh              chan ApplyMsg
	cond                 *sync.Cond
	matchCount           map[int]int
	lastIncludedLogIndex int
	lastIncludedLogTerm  int
	applyTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isLeader bool
	// Your code here (2A).
	if rf.state == 3 {
		isLeader = true
	}
	term = rf.currentTerm
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	data := rf.encodeRaftState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	encoder.Encode(rf.lastIncludedLogIndex)
	encoder.Encode(rf.lastIncludedLogTerm)
	return w.Bytes()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastIncludedLogIndex int
	var lastIncludedLogTerm int

	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&logs) != nil ||
		decoder.Decode(&lastIncludedLogIndex) != nil ||
		decoder.Decode(&lastIncludedLogTerm) != nil {
		log.Fatalf("[%d] Failed to decode persisted states.", rf.me)
	} else {
		//DPrintf("[%d] persisted log len: %d", rf.me, len(logs))
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		rf.lastIncludedLogIndex = lastIncludedLogIndex
		rf.lastIncludedLogTerm = lastIncludedLogTerm
	}
}

// checks if current commitIndex > lastApplied when notified that commit index is changed
// if true, apply new committed log to state machine
// if the server is the leader, check if there is new logs to commit first
func (rf *Raft) applier() {
	rf.mu.Lock()
	for !rf.killed() {
		rf.cond.Wait()
		rf.applyCommand()
	}
	rf.mu.Unlock()
}

func (rf *Raft) applyCommand() {
	DPrintf("[%d] last applied: %d", rf.me, rf.lastApplied)
	if rf.state == leader {
		DPrintf("match index of 1 is: %d", rf.matchIndex[1])
		for i := len(rf.log) - 1; i >= 0; i-- {
			DPrintf("%d %d", rf.log[i].Index, rf.commitIndex)
			if rf.log[i].Index == rf.commitIndex {
				break
			}

			if rf.log[i].Term < rf.currentTerm {
				break
			}

			count := 1
			for j := range rf.matchIndex {
				if rf.matchIndex[j] >= rf.log[i].Index {
					count++
				}
			}

			if count > len(rf.peers)/2 {
				rf.commitIndex = rf.log[i].Index
				//DPrintf("[%d]new commit index: %d",rf.me, rf.commitIndex)
				break
			}
		}
	}

	DPrintf("[%d]updating commit index, lastApplied: %d, lastIncludedIndex: %d, logLen: %d", rf.me, rf.lastApplied, rf.lastIncludedLogIndex, len(rf.log))
	firstLogIndex := rf.getFirstLogIndex()
	for rf.lastApplied < rf.commitIndex {
		index := rf.lastApplied + 1 - firstLogIndex
		l := rf.log[index]
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      l.Command,
			CommandIndex: l.Index,
			CommandTerm:  l.Term,
		}
		rf.lastApplied++
		if rf.state == leader {
			DPrintf("[%d] applying log... ", rf.me)
		}
		rf.applyCh <- applyMsg
		if rf.state == leader {
			DPrintf("[%d] log applied... ", rf.me)
		}
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := true

	if rf.state != leader {
		isLeader = false
		return index, term, isLeader
	}

	lastLogIndex, _ := rf.getLastLogInfo()
	index = lastLogIndex + 1
	term = rf.currentTerm

	DPrintf("[%d] get new command, index: %d, command: %s, lastApplied: %d", rf.me, index, command, rf.lastApplied)

	newLog := LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}

	rf.log = append(rf.log, newLog)
	go rf.persist()
	defer func() {
		rf.timer.Reset(5 * time.Millisecond)
	}()

	return index, term, isLeader
}

// keeps check of both election timeout and
// heartbeat timeout in a separated goroutine
func (rf *Raft) timeout() {
	for !rf.killed() {
		<-rf.timer.C
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state == leader {
			rf.sendHeartBeat()
		} else {
			go rf.startElection()
			//DPrintf("[%d] is starting an election...", rf.me)
			//rf.timer.Reset(getRandomTimeout())
		}
	}
}

// get firstLogIndex, if the length of the log is 0, return -1
// lock is required by the caller
func (rf *Raft) getFirstLogIndex() int {
	if len(rf.log) == 0 {
		return -1
	}
	return rf.log[0].Index
}

// get lastLogIndex, if the length of the log is 0, return lastIncludedIndex in the last snapshot
// lock is required by the caller
func (rf *Raft) getLastLogInfo() (int, int) {
	if len(rf.log) == 0 {
		return rf.lastIncludedLogIndex, rf.lastIncludedLogTerm
	}
	return rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term
}

// when sending appendEntries request to peers, use this function to get prevLogIndex and prevLogTerm
// for AppendEntriesArgs
func (rf *Raft) getPrevLogInfo(index int) (int, int) {
	if index == 0 {
		return rf.lastIncludedLogIndex, rf.lastIncludedLogTerm
	}
	return rf.log[index-1].Index, rf.log[index-1].Term
}

// for kv server to monitor raft state size
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
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

// generates a random timeout time
func (rf *Raft) getRandomTimeout() time.Duration {
	t := time.Millisecond * time.Duration(minTimeout+rand.Intn(maxTimeout-minTimeout))
	//DPrintf("[%d] election timeout is %d", rf.me, t)
	return t
}

func (rf *Raft) readSnapshot() {
	data := rf.persister.ReadSnapshot()

	if data != nil {
		DPrintf("[%d] restoring snapshot", rf.me)
		rf.lastApplied = rf.lastIncludedLogIndex
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      data,
			IsSnapshot:   true,
		}
		go func() {
			rf.mu.Lock()
			rf.applyCh <- applyMsg
			rf.mu.Unlock()
		}()
	}
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
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = append(rf.log, LogEntry{})
	rf.state = follower
	rf.timer = time.NewTimer(rf.getRandomTimeout())
	rf.cond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh
	rf.applyTimer = time.NewTimer(applyTimeout * time.Millisecond)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot()

	go rf.applier()
	go rf.timeout()
	go func() {
		<- rf.applyTimer.C
		rf.mu.Lock()
		rf.applyCommand()
		rf.mu.Unlock()
		rf.applyTimer.Reset(applyTimeout * time.Millisecond)
	}()

	return rf
}
