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

	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	state       int
	timer       *time.Timer
	applyCh     chan ApplyMsg
	cond        *sync.Cond
	matchCount  map[int]int
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
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
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
	r := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&logs) != nil {
		log.Fatalf("[%d] Failed to decode persisted states.", rf.me)
	} else {
		DPrintf("[%d] persisted log len: %d", rf.me, len(logs))
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
	}
}

// checks if current commitIndex > lastApplied when notified that commit index is changed
// if true, apply new committed log to state machine
// if the server is the leader, check if there is new logs to commit first
func (rf *Raft) applier() {

	rf.mu.Lock()
	for !rf.killed() {
		rf.cond.Wait()

		lastLog := rf.log[len(rf.log)-1]
		if rf.state == leader {
			for i := lastLog.Index; i > rf.commitIndex; i-- {
				if rf.log[i].Term < rf.currentTerm {
					break
				}

				count := 1
				for j := range rf.matchIndex {
					if rf.matchIndex[j] >= i {
						count++
					}
				}

				if count > len(rf.peers)/2 {
					rf.commitIndex = i
					//DPrintf("[%d]new commit index: %d",rf.me, rf.commitIndex)
					break
				}
			}
		}

		//DPrintf("[%d]updating commit index...", rf.me)

		for rf.lastApplied < rf.commitIndex {
			l := rf.log[rf.lastApplied+1]
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      l.Command,
				CommandIndex: l.Index,
			}
			rf.lastApplied++
			rf.applyCh <- applyMsg
		}
	}
	rf.mu.Unlock()
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
	lastLog := rf.log[len(rf.log)-1]
	index := -1
	term := -1
	isLeader := true

	if rf.state != leader {
		isLeader = false
		return index, term, isLeader
	}

	index = lastLog.Index + 1
	term = rf.currentTerm

	DPrintf("[%d] get new command, index: %d, command: %s", rf.me, index, command)

	newLog := LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}

	rf.log = append(rf.log, newLog)
	go rf.persist()

	// Your code here (2B).

	return index, term, isLeader
}

// keeps check of both election timeout and
// heartbeat timeout in a separated goroutine
func (rf *Raft) timeout() {
	for !rf.killed() {
		<-rf.timer.C
		if rf.state == leader {
			rf.sendHeartBeat()
		} else {
			go rf.startElection()
			DPrintf("[%d] is starting an election...", rf.me)
			//rf.timer.Reset(getRandomTimeout())
		}
	}
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.applier()
	go rf.timeout()

	return rf
}
