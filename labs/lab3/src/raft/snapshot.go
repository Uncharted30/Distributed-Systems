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
	Status bool
	Term int
}

// snapshot state
func (rf *Raft) Snapshot(snapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.lastIncludedLogIndex >= index {
		return
	}
	//DPrintf("[%d] logs: %s", rf.me, rf.log)
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
	DPrintf("[%d] state size: %d", rf.me, len(state))

	rf.persister.SaveStateAndSnapshot(state, snapshot)
	rf.lastIncludedLogIndex = lastIncludedLogIndex
	rf.lastIncludedLogTerm = lastIncludedLogTerm

	// Discard logs
	DPrintf("[%d] Snapshot taken, current first log index: %d, last included index: %d", rf.me, firstLogIndex, lastIncludedLogIndex)
	rf.log = rf.log[lastIncludedIndexInArr+1:]
}

// InstallSnapshot RPC
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// wrong term or last snapshot in the raft server is newer
	if reply.Term > args.Term || rf.lastIncludedLogIndex >= args.LastIncludedIndex {
		reply.Status = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	applyMsg := ApplyMsg{
		CommandValid: true,
		Command:      args.Data,
		IsSnapshot:   true,
	}

	// update snapshot info
	rf.lastIncludedLogIndex = args.LastIncludedIndex
	rf.lastIncludedLogTerm = args.LastIncludedTerm
	firstLogIndex := rf.getFirstLogIndex()
	reply.Status = true
	DPrintf("[%d] Installing snapshot, firstLogIndex: %d, logLen: %d", rf.me, firstLogIndex, len(rf.log))
	lastIncludedLogIndexInArr := args.LastIncludedIndex - firstLogIndex

	// check if the snapshot describes a prefix of this raft server's log
	// if there's no log in this raft server, the snapshot contains new information
	if firstLogIndex != -1 && lastIncludedLogIndexInArr < len(rf.log) {
		// the Raft paper is not clear about how to deal with the case where the snapshot describes a prefix of
		// this server's log, if we do not send the snapshot to kv server and delete entries before the last included
		// index, this server may not know that the deleted log is committed and applied, so I also send the snapshot
		// to kv server even if the snapshot describes a prefix of its log
		if rf.log[lastIncludedLogIndexInArr].Term == args.LastIncludedTerm {
			rf.log = rf.log[lastIncludedLogIndexInArr+1:]
			if rf.lastApplied < args.LastIncludedIndex {
				rf.lastApplied = args.LastIncludedIndex
				DPrintf("[%d] sending snapshot to applyCh", rf.me)
				rf.applyCh <- applyMsg
			}

			if rf.commitIndex < args.LastIncludedIndex {
				rf.commitIndex = args.LastIncludedIndex
			}
			DPrintf("[%d] exiting", rf.me)
		}
	} else {
		rf.log = make([]LogEntry, 0)
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = args.LastIncludedIndex
		DPrintf("[%d] sending snapshot to applyCh", rf.me)
		rf.applyCh <- applyMsg
	}

	DPrintf("[%d] encoding state and snapshot...", rf.me)
	state := rf.encodeRaftState()
	rf.persister.SaveStateAndSnapshot(state, args.Data)
}

func (rf *Raft) sendSnapshot(server int) {

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
	DPrintf("[%d] Sending snapshot to %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)

	if ok {
		rf.mu.Lock()
		if reply.Status {
			if rf.nextIndex[server] < rf.lastIncludedLogIndex + 1 {
				rf.nextIndex[server] = rf.lastIncludedLogIndex + 1
			}
		} else {
			if reply.Term > rf.currentTerm {
				rf.state = follower
				rf.currentTerm = reply.Term
				DPrintf("[%d] current term: %d, reply term: %d, args term: %d", rf.me, rf.currentTerm, reply.Term, args.Term)
			}
		}
		rf.mu.Unlock()
	} else {
		DPrintf("[%d] sending snapshot failed", rf.me)
	}
}
