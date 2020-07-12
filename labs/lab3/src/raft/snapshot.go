package raft

import (
	"bytes"
)
import "../labgob"

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// snapshot state
func (rf *Raft) Snapshot(snapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	firstLogIndex := rf.getFirstLogIndex()

	lastIncludedIndexInArr := index - firstLogIndex
	lastIncludedLogIndex := index
	lastIncludedLogTerm := rf.log[lastIncludedIndexInArr].Term


	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log[lastIncludedIndexInArr+1:])
	encoder.Encode(lastIncludedLogIndex)
	encoder.Encode(lastIncludedLogTerm)
	state := w.Bytes()

	rf.persister.SaveStateAndSnapshot(state, snapshot)
	rf.lastIncludedLogIndex = lastIncludedLogIndex
	rf.lastIncludedLogTerm = lastIncludedLogTerm
	// Discard logs

	if len(rf.log) > 0 {
		firstLogIndex = -1
	} else {
		firstLogIndex = rf.log[0].Index
	}
	DPrintf("[%d] Snapshot taken, current first log index: %d, last included index: %d", rf.me, firstLogIndex, lastIncludedLogIndex)
	rf.log = rf.log[lastIncludedIndexInArr+1:]
}

// InstallSnapshot RPC
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("[%d] Installing snapshot", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// wrong term
	if reply.Term > args.Term {
		return
	}

	rf.lastIncludedLogIndex = args.LastIncludedIndex
	rf.lastIncludedLogTerm = args.LastIncludedTerm

	lastIncludedLogIndexInArr := rf.log[0].Index - args.LastIncludedIndex
	if lastIncludedLogIndexInArr >= 0 {
		if rf.log[lastIncludedLogIndexInArr].Term == args.LastIncludedTerm {
			rf.log = rf.log[lastIncludedLogIndexInArr+1:]
			state := rf.encodeRaftState()
			rf.persister.SaveStateAndSnapshot(state, args.Data)
			return
		}
	} else {
		rf.log = make([]LogEntry, 0)
	}
	state := rf.encodeRaftState()
	rf.persister.SaveStateAndSnapshot(state, args.Data)

	applyMsg := ApplyMsg{
		CommandValid: true,
		Command:      args.Data,
		IsSnapshot:   true,
	}

	rf.applyCh <- applyMsg
}

func (rf *Raft) sendSnapshot(server int) {
	DPrintf("[%d] Sending snapshot to %d", rf.me, server)
	rf.mu.Lock()

	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedLogIndex,
		LastIncludedTerm:  rf.lastIncludedLogTerm,
		Data:              rf.persister.ReadSnapshot(),
	}

	reply := InstallSnapshotReply{}

	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)

	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.state = follower
			rf.currentTerm = reply.Term
		} else {
			rf.nextIndex[server] = rf.lastIncludedLogIndex + 1
		}
		rf.mu.Unlock()
	} else {
		DPrintf("[%d] sending snapshot failed", rf.me)
	}
}
