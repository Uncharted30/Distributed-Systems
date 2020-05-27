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
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

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
	minTimeout = 500
	maxTimeout = 650
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
	matchCount map[int]int
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

// append entries RPC arguments structure
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// append entries RPC reply structure
type AppendEntriesReply struct {
	Term    int
	Success bool
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

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = follower
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm

	// first check term of the candidate
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		DPrintf("[%d] rejected vote request from %d, because current term is larger.", rf.me, args.CandidateId)
		return
	} else {
		//check if the server has granted vote for this term
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			// check if the candidate's log is up-to-date
			lastLog := rf.log[len(rf.log)-1]
			if lastLog.Term == args.LastLogTerm {
				if lastLog.Index <= args.LastLogIndex {
					reply.VoteGranted = true
					rf.votedFor = args.CandidateId
					rf.timer.Reset(rf.getRandomTimeout())
					return
				} else {
					reply.VoteGranted = false
					DPrintf("[%d] rejected vote request from %d, because log is more up-to-date.", rf.me, args.CandidateId)
					return
				}
			} else if lastLog.Term < args.LastLogTerm {
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				rf.timer.Reset(rf.getRandomTimeout())
				return
			} else {
				reply.VoteGranted = false
				return
			}
		} else { // this server has granted vote to another candidate
			reply.VoteGranted = false
			DPrintf("[%d] rejected vote request from %d, because already voted.", rf.me, args.CandidateId)
			return
		}
	}
}

// Append entries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.timer.Reset(rf.getRandomTimeout())
	DPrintf("[%d] get AppendEntries request from %d, current commit index: %d, last applied: %d, term: %d", rf.me, args.LeaderId,rf.commitIndex, rf.lastApplied, rf.currentTerm)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm

	lastLogEntry := rf.log[len(rf.log)-1]
	//check if this server has the PrevLog
	//DPrintf("%d %d, %d %d", lastLogEntry.Index, lastLogEntry.Term, args.PrevLogIndex, args.PrevLogTerm)

	if lastLogEntry.Index < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	reply.Success = true

	DPrintf("[%d]appending new log entries... len: %d", rf.me, len(args.Entries))
	logLen := len(rf.log)
	for _, log := range args.Entries {
		if log.Index < logLen {
			if rf.log[log.Index].Term != log.Term {
				rf.log = rf.log[0:log.Index]
				logLen = len(rf.log)
			}
		}
		rf.log = append(rf.log, log)
	}

	lastLogEntry = rf.log[len(rf.log)-1]
	if args.LeaderCommit > rf.commitIndex {
		if lastLogEntry.Index < args.LeaderCommit {
			rf.commitIndex = lastLogEntry.Index
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.cond.Broadcast()
	}


}

// send heartbeat message to peers
func (rf *Raft) sendHeartBeat() {
	for i := range rf.peers {

		if i == rf.me {
			continue
		}

		//DPrintf("[%d] is sending heartbeat message to %d", rf.me, i)
		rf.mu.Lock()

		lastLog := rf.log[len(rf.log)-1]

		var entries []LogEntry
		if rf.nextIndex[i] <= lastLog.Index {
			entries = rf.log[rf.nextIndex[i]:]
		} else {
			entries = make([]LogEntry, 0)
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()
		reply := new(AppendEntriesReply)
		go rf.sendAppendEntries(i, &args, reply)
	}

	rf.timer.Reset(heartbeat * time.Millisecond)
}

//
// send a RequestVote RPC to a server.
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
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = follower
		}
	}
	return ok
}

// send an appendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		if args.Term != rf.currentTerm {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			//DPrintf("old matchIndex: %d, old nextIndex: %d", rf.matchIndex[server], rf.nextIndex[server])
			if len(args.Entries) > 0 {
				DPrintf("[%d]get AppendEntry reply from %d", rf.me, server)
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				rf.cond.Broadcast()
			}
			//DPrintf("new matchIndex: %d, new nextIndex: %d", rf.matchIndex[server], rf.nextIndex[server])
		} else {
			if reply.Term == args.Term {
				rf.nextIndex[server]--
				//DPrintf("new matchIndex: %d, new nextIndex: %d", rf.matchIndex[server], rf.nextIndex[server])
				rf.mu.Unlock()
				reply = new(AppendEntriesReply)
				args.LeaderCommit = rf.commitIndex
				args.PrevLogTerm = rf.log[rf.nextIndex[server] - 1].Term
				args.PrevLogIndex = rf.nextIndex[server] - 1
				args.Entries = rf.log[rf.nextIndex[server]:]
				go rf.sendAppendEntries(server, args, reply)
				return
			} else if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = follower
				rf.votedFor = -1
				rf.timer.Reset(rf.getRandomTimeout())
			}
		}
		rf.mu.Unlock()
	}
}

// checks if current commitIndex > lastApplied periodically
// if true, apply new committed log to state machine
// if the server is the leader, check if there is new logs to commit first
func (rf *Raft) applier() {

	rf.mu.Lock()
	for !rf.killed() {
		rf.cond.Wait()

		lastLog := rf.log[len(rf.log) - 1]
		if rf.state == leader {
			for i := lastLog.Index; i > rf.commitIndex ; i-- {
				count := 1
				for j := range rf.matchIndex {
					DPrintf("%d %d", j, rf.matchIndex[j])
					if rf.matchIndex[j] >= i {
						count++
					}
				}

				if count > len(rf.peers)/2 {
					rf.commitIndex = i
					DPrintf("new commit index: %d", rf.commitIndex)
					break
				}
			}
		}

		DPrintf("updating commit index...")

		for rf.lastApplied < rf.commitIndex {
			log := rf.log[rf.lastApplied+1]
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      log.Command,
				CommandIndex: log.Index,
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

	DPrintf("[%d] get new command, index: %d", rf.me, index)

	newLog := LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}

	rf.log = append(rf.log, newLog)

	// Your code here (2B).

	return index, term, isLeader
}

// start a new election
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = candidate
	rf.timer.Reset(rf.getRandomTimeout())
	lastLog := rf.log[len(rf.log)-1]
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	rf.mu.Unlock()
	rf.requestVotes(&args)
}

func (rf *Raft) requestVotes(args *RequestVoteArgs) {
	// vote received
	vote := 1
	// reply received
	finished := 1
	// lock of vote and total
	lock := sync.Mutex{}
	// condition variable
	cond := sync.NewCond(&lock)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := new(RequestVoteReply)
			DPrintf("[%d] is sending vote request to [%d]", rf.me, server)
			ok := rf.sendRequestVote(server, args, reply)
			if ok {
				lock.Lock()
				defer lock.Unlock()
				DPrintf("[%d] get result from %d", rf.me, server)
				finished++
				if reply.VoteGranted {
					vote++
					DPrintf("[%d(%d)] get vote from %d, ", rf.me, rf.state, server)
				}
				cond.Broadcast()
			}
		}(i)
	}

	lock.Lock()
	for vote <= len(rf.peers)/2 && finished < 10 {
		cond.Wait()
		rf.mu.Lock()
		if rf.state != follower || args.Term != rf.currentTerm {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}

	rf.mu.Lock()
	if rf.state == candidate && args.Term == rf.currentTerm {
		if vote > len(rf.peers)/2 {
			rf.mu.Unlock()
			DPrintf("[%d] became the leader", rf.me)
			rf.becomeLeader()
			return
		}
	}
	rf.mu.Unlock()
}

// step up as a leader
func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	lastLog := rf.log[len(rf.log)-1]
	rf.state = leader
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = lastLog.Index + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.mu.Unlock()
	rf.sendHeartBeat()
	rf.timer.Reset(heartbeat * time.Millisecond)
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
