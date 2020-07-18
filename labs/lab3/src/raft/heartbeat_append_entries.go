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

	lastLogIndex, _ := rf.getLastLogInfo()
	firstLogIndex := rf.getFirstLogIndex()
	// to locate prevLog in the args after snapshotting
	prevLogIndexInArr := args.PrevLogIndex - firstLogIndex
	//DPrintf("%d %d, %d %d", lastLogEntry.Index, lastLogEntry.Term, args.PrevLogIndex, args.PrevLogTerm)
	DPrintf("[%d - %d] get AppendEntries request from %d, current commit index: %d, last applied: %d, term: %d, entries len: %d, prevLogIndex: %d, lastLogIndex: %d, current log len: %d", rf.me, rf.state, args.LeaderId, rf.commitIndex, rf.lastApplied, rf.currentTerm, len(args.Entries), args.PrevLogIndex, lastLogIndex, len(rf.log))

	reply.Success = false
	reply.XLen = lastLogIndex + 1
	reply.XIndex = -1
	reply.XTerm = -1

	if firstLogIndex == -1 {
		// if there's no log entry in the log array because of snapshotting
		// check if lastIncludedIndex is PrevLogIndex in the request args
		if lastLogIndex != args.PrevLogIndex {
			return
		}
	} else {
		// no log entry in given preLogIndex
		if lastLogIndex < args.PrevLogIndex {
			return
		} else {
			// if prevLogIndexInArr is smaller than 0
			// it is in the last snapshot of this rf server
			// so we already have that log entry
			// we just need to worry about the situation when it is larger than or equal to 0
			if prevLogIndexInArr >= 0 && rf.log[prevLogIndexInArr].Term != args.PrevLogTerm {
				reply.XTerm = rf.log[prevLogIndexInArr].Term
				i := prevLogIndexInArr - 1
				for ; i >= 0; i-- {
					if rf.log[i].Term != reply.XTerm {
						break
					}
				}
				// find the index of first log in conflicting term
				if i >= 0 {
					reply.XIndex = rf.log[i].Index + 1
				} else {
					if rf.lastIncludedLogTerm == reply.XTerm {
						reply.XIndex = rf.lastIncludedLogIndex + 1
					} else {
						reply.XIndex = rf.log[0].Index + 1
					}
				}

				return
			}
		}
	}

	reply.Success = true

	// append new entries to the log in this server
	for _, l := range args.Entries {
		if l.Index <= lastLogIndex {
			// if there's term conflict, delete all entries from that entry index
			logIndex := l.Index - firstLogIndex
			// if logIndex < 0, it's already in the latest snapshot
			if logIndex >= 0 && rf.log[logIndex].Term != l.Term {
				rf.log = rf.log[0:logIndex]
				lastLogIndex, _ = rf.getLastLogInfo()
			} else {
				continue
			}
		}
		rf.log = append(rf.log, l)
		changed = true
	}

	// update commit index
	lastLogIndex, _ = rf.getLastLogInfo()
	if args.LeaderCommit > rf.commitIndex {
		if lastLogIndex < args.LeaderCommit {
			rf.commitIndex = lastLogIndex
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

		lastLogIndex, _ := rf.getLastLogInfo()
		firstLogIndex := rf.getFirstLogIndex()

		// new log entries to be sent to peers
		var entries []LogEntry
		nextIndexInArr := rf.nextIndex[i] - firstLogIndex
		var prevLogIndex, prevLogTerm int

		if rf.nextIndex[i] <= lastLogIndex {
			// checks if sending snapshot is needed, if log len in this raft server is 0
			// nextIndex[i] must be included in latest snapshot since lastLogIndex == lastIncludedLogIndex
			if firstLogIndex == -1 || rf.nextIndex[i] < firstLogIndex {
				go rf.sendSnapshot(i)
				rf.mu.Unlock()
				continue
			}
			entries = rf.log[nextIndexInArr:]
			prevLogIndex, prevLogTerm = rf.getPrevLogInfo(nextIndexInArr)
		} else {
			entries = make([]LogEntry, 0)
			prevLogIndex, prevLogTerm = rf.getLastLogInfo()
		}

		DPrintf("[%d] Sending appendEntries to %d, lastLogIndex: %d, nextIndex: %d, logLen: %d", rf.me, i, lastLogIndex, rf.nextIndex[i], len(rf.log))

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
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

		DPrintf("[%d]get AppendEntry reply from %d, result: %s", rf.me, server, reply.Success)

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

				firstLogIndex := rf.getFirstLogIndex()

				if firstLogIndex == -1 || rf.nextIndex[server] < firstLogIndex {
					DPrintf("[%d] nextIndex: %d, firstLogIndex: %d", rf.me, rf.nextIndex, firstLogIndex)
					go rf.sendSnapshot(server)
					rf.mu.Unlock()
					return
				}

				nextLogIndex := rf.nextIndex[server] - firstLogIndex
				prevLogIndex, prevLogTerm := rf.getPrevLogInfo(nextLogIndex)
				reply = new(AppendEntriesReply)
				args.LeaderCommit = rf.commitIndex
				args.PrevLogTerm = prevLogTerm
				args.PrevLogIndex = prevLogIndex
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
