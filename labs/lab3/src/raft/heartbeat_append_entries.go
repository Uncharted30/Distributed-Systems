package raft

import "time"

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
	XTerm int
	XIndex int
	XLen int
}

// Append entries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	changed := false

	//DPrintf("[%d - %d] get AppendEntries request from %d, current commit index: %d, last applied: %d, term: %d, entries len: %d, prevLogIndex: %d", rf.me, rf.state, args.LeaderId, rf.commitIndex, rf.lastApplied, rf.currentTerm, len(args.Entries), args.PrevLogIndex)

	// checks if term of the master is at least as up-to-date as this server
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.timer.Reset(rf.getRandomTimeout())

	// update term
	if args.Term > rf.currentTerm {
		rf.state = follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		changed = true
	}

	reply.Term = rf.currentTerm

	lastLogEntry := rf.log[len(rf.log)-1]
	firstLogIndex := rf.log[0].Index
	// to locate prevLog in the args after snapshotting
	thisPreLogIndex := args.PrevLogIndex - firstLogIndex
	//DPrintf("%d %d, %d %d", lastLogEntry.Index, lastLogEntry.Term, args.PrevLogIndex, args.PrevLogTerm)

	// checks if this server has lastLogEntry
	// with quick roll back
	if lastLogEntry.Index < args.PrevLogIndex || rf.log[thisPreLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.XLen = lastLogEntry.Index + 1

		if lastLogEntry.Index < args.PrevLogIndex {
			reply.XIndex = -1
			reply.XTerm = -1
		} else {
			reply.XTerm = rf.log[thisPreLogIndex].Term
			i := rf.log[thisPreLogIndex].Index - 1
			for ; i > 0; i-- {
				if rf.log[i].Term != reply.XTerm {
					break
				}
			}
			reply.XIndex = i + 1
		}
		return
	}

	reply.Success = true

	// append new entries to the log in this server
	for _, l := range args.Entries {
		if l.Index <= lastLogEntry.Index {
			// if there's term conflict, delete all entries from that entry index
			logIndex := l.Index - firstLogIndex
			if rf.log[logIndex].Term != l.Term {
				rf.log = rf.log[0:logIndex]
				lastLogEntry = rf.log[len(rf.log) - 1]
			} else {
				continue
			}
		}
		rf.log = append(rf.log, l)
		changed = true
	}

	// update commit index
	lastLogEntry = rf.log[len(rf.log)-1]
	if args.LeaderCommit > rf.commitIndex {
		if lastLogEntry.Index < args.LeaderCommit {
			rf.commitIndex = lastLogEntry.Index
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.cond.Broadcast()
	}

	if changed {
		go rf.persist()
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
		firstLogIndex := rf.log[0].Index

		// new log entries to be sent to peers
		var entries []LogEntry
		nextIndex := rf.nextIndex[i] - firstLogIndex
		if rf.nextIndex[i] <= lastLog.Index {
			entries = rf.log[nextIndex:]
		} else {
			entries = make([]LogEntry, 0)
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.log[nextIndex - 1].Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()
		reply := new(AppendEntriesReply)
		go rf.sendAppendEntries(i, &args, reply)
	}

	rf.timer.Reset(heartbeat * time.Millisecond)
}

// send an appendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//DPrintf("[%d] sending AppendEntries to %d, commit index is %d, log len: %d, term: %d", rf.me, server, rf.commitIndex, len(rf.log), rf.currentTerm)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		// checks if the master stepped down when waiting for the reply
		if args.Term != rf.currentTerm {
			rf.mu.Unlock()
			return
		}

		//DPrintf("[%d]get AppendEntry reply from %d", rf.me, server)

		if reply.Success {
			if len(args.Entries) > 0 {
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				rf.cond.Broadcast()
			}
		} else {
			if reply.Term == args.Term {
				// quick rollback
				if reply.XTerm == -1 {
					rf.nextIndex[server] = reply.XLen
				} else {
					hasTerm := false
					i := len(rf.log) - 1
					for ; i >= 0; i-- {
						if rf.log[i].Term == reply.XTerm {
							hasTerm = true
							break
						} else if rf.log[i].Term < reply.XTerm {
							break
						}
					}

					if hasTerm {
						rf.nextIndex[server] = i
					} else {
						rf.nextIndex[server] = reply.XIndex
					}
				}

				nextLogIndex := rf.nextIndex[server] - rf.log[0].Index
				reply = new(AppendEntriesReply)
				args.LeaderCommit = rf.commitIndex
				args.PrevLogTerm = rf.log[nextLogIndex - 1].Term
				args.PrevLogIndex = nextLogIndex - 1
				args.Entries = rf.log[nextLogIndex:]
				rf.mu.Unlock()
				go rf.sendAppendEntries(server, args, reply)
				return
			} else if reply.Term > rf.currentTerm {
				//DPrintf("[%d] stepping down...", rf.me)
				rf.currentTerm = reply.Term
				rf.state = follower
				rf.votedFor = -1
				rf.timer.Reset(rf.getRandomTimeout())
				go rf.persist()
			}
		}
		rf.mu.Unlock()
	} else {
		//DPrintf("[%d] request to %d failed", rf.me, server)
	}
}
