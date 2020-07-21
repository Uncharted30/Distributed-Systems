package raft

import (
	"sync"
	"time"
)

//
// RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if the candidate's term is larger than this server's term
	// update term and step down
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = follower
		rf.votedFor = -1
		rf.persist()
	}
	reply.Term = rf.currentTerm

	// first check term of the candidate
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		//DPrintf("[%d] rejected vote request from %d, because current term is larger.", rf.me, args.CandidateId)
		return
	} else {
		//check if the server has granted vote for this term
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			// check if the candidate's log is up-to-date
			lastLogIndex, lastLogTerm := rf.getLastLogInfo()
			if lastLogTerm == args.LastLogTerm {
				if lastLogIndex <= args.LastLogIndex {
					//DPrintf("[%d] grand vote to %d", rf.me, args.CandidateId)
					reply.VoteGranted = true
					rf.votedFor = args.CandidateId
					rf.timer.Reset(rf.getRandomTimeout())
					rf.persist()
					return
				} else {
					reply.VoteGranted = false
					//DPrintf("[%d] rejected vote request from %d, because log is more up-to-date.", rf.me, args.CandidateId)
					return
				}
			} else if lastLogTerm < args.LastLogTerm {
				//DPrintf("[%d] grand vote to %d", rf.me, args.CandidateId)
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				rf.timer.Reset(rf.getRandomTimeout())
				rf.persist()
				return
			} else {
				reply.VoteGranted = false
				return
			}
		} else { // this server has granted vote to another candidate
			reply.VoteGranted = false
			//DPrintf("[%d] rejected vote request from %d, because already voted.", rf.me, args.CandidateId)
			return
		}
	}
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
			rf.persist()
		}
	}
	return ok
}

// start a new election
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = candidate
	rf.timer.Reset(rf.getRandomTimeout())
	lastLogIndex, lastLogTerm := rf.getLastLogInfo()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()
	rf.persist()
	rf.requestVotes(&args)
}

func (rf *Raft) requestVotes(args *RequestVoteArgs) {
	// vote received
	vote := 1
	// reply received
	finished := 1
	// lock of vote and finished
	lock := sync.Mutex{}
	// condition variable
	cond := sync.NewCond(&lock)

	for i := range rf.peers {
		// skip self
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := new(RequestVoteReply)
			//DPrintf("[%d] is sending vote request to [%d]", rf.me, server)
			ok := rf.sendRequestVote(server, args, reply)
			if ok {
				lock.Lock()
				defer lock.Unlock()
				//DPrintf("[%d] get result from %d", rf.me, server)
				finished++
				if reply.VoteGranted {
					vote++
					//DPrintf("[%d(%d)] get vote from %d, ", rf.me, rf.state, server)
				}
				cond.Broadcast()
			}
		}(i)
	}

	lock.Lock()
	for vote <= len(rf.peers)/2 && finished < len(rf.peers) {
		cond.Wait()
		//DPrintf("[%d] votes get: %d", rf.me, vote)
		rf.mu.Lock()
		if rf.state != candidate || args.Term != rf.currentTerm {
			//DPrintf("[%d] exit election because of term change", rf.me)
			rf.mu.Unlock()
			lock.Unlock()
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
	lastLogIndex, _ := rf.getLastLogInfo()
	rf.state = leader
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = lastLogIndex+ 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.mu.Unlock()
	rf.Start(nil)
	rf.sendHeartBeat()
	rf.timer.Reset(heartbeat * time.Millisecond)
}