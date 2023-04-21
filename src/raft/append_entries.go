package raft

import "time"

// AppendEntriesArgs is the AppendEntries RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // with leaderId follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

// AppendEntriesReply is the AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	XTerm   int  // term in the conflicting entry (if any)
	XIndex  int  // index of first entry with that term (if any)
	XLen    int  // log length
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// code to send a AppendEntries RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries) == 0 {
		Debug(dLog2, "S%d <- S%d Received heartbeat at T%d.", rf.me, args.LeaderId, rf.currentTerm)
	} else {
		Debug(dLog2, "S%d <- S%d Received append entries at T%d.", rf.me, args.LeaderId, rf.currentTerm)
	}
	reply.Success = false

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		Debug(dLog2, "S%d Term is lower, rejecting append request. (%d < %d)",
			rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}

	// For Candidates. If AppendEntries RPC received from new leader: convert to follower
	if rf.state == CandidateState && rf.currentTerm == args.Term {
		rf.state = FollowerState
		Debug(dLog2, "S%d Convert from candidate to follower at T%d.", rf.me, rf.currentTerm)
	}

	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm

	Debug(dTimer, "S%d Resetting ELT, wait for next potential heartbeat timeout.", rf.me)
	rf.setElectionTimeout(randHeartbeatTimeout())

	rf.leaderId = args.LeaderId

	if args.PrevLogIndex < rf.lastIncludedIndex {
		alreadySnapshotLogLen := rf.lastIncludedIndex - args.PrevLogIndex
		if alreadySnapshotLogLen <= len(args.Entries) {
			newArgs := &AppendEntriesArgs{
				Term:         args.Term,
				LeaderId:     args.LeaderId,
				PrevLogTerm:  rf.lastIncludedTerm,
				PrevLogIndex: rf.lastIncludedIndex,
				Entries:      args.Entries[alreadySnapshotLogLen:],
				LeaderCommit: args.LeaderCommit,
			}
			args = newArgs
			Debug(dWarn, "S%d Log entry at PLI already discarded by snapshot, readjusting. PLI: %d, PLT:%d, Entries: %v.",
				rf.me, args.PrevLogIndex, args.Entries)
		} else {
			Debug(dWarn, "S%d Log entry at PLI already discarded by snapshot, assume as a match. PLI: %d.", rf.me, args.PrevLogIndex)
			reply.Success = true
			return
		}
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogTerm == -1 || args.PrevLogTerm != rf.getEntry(args.PrevLogIndex).Term {
		Debug(dDrop, "S%d Prev log entries do not match. Ask leader to retry.", rf.me)
		reply.XLen = len(rf.log) + rf.lastIncludedIndex
		reply.XTerm = rf.getEntry(args.PrevLogIndex).Term
		reply.XIndex, _ = rf.getBoundsWithTerm(reply.XTerm)
		return
	}
	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	for i, entry := range args.Entries {
		if rf.getEntry(i+1+args.PrevLogIndex).Term != entry.Term {
			rf.log = append(rf.getSlice(1+rf.lastIncludedIndex, i+1+args.PrevLogIndex), args.Entries[i:]...)
			break
		}
	}
	Debug(dLog2, "S%d <- S%d Append entries success. Saved logs: %v.", rf.me, args.LeaderId, args.Entries)
	if len(args.Entries) > 0 {
		rf.persist()
	}
	if args.LeaderCommit > rf.commitIndex {
		Debug(dCommit, "S%d Get higher LC at T%d, updating commitIndex. (%d < %d)",
			rf.me, rf.currentTerm, rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		Debug(dCommit, "S%d Updated commitIndex at T%d. CI: %d.", rf.me, rf.currentTerm, rf.commitIndex)
	}
	reply.Success = true
}

func (rf *Raft) sendEntries(isHeartbeat bool) {
	Debug(dTimer, "S%d Resetting HBT, wait for next heartbeat broadcast.", rf.me)
	rf.setHeartbeatTimeout(time.Duration(HeartbeatInterval) * time.Millisecond)
	Debug(dTimer, "S%d Resetting ELT, wait for next potential heartbeat timeout.", rf.me)
	rf.setElectionTimeout(randHeartbeatTimeout())
	lastLogIndex, _ := rf.lastLogInfo()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		nextIndex := rf.nextIndex[peer]
		if nextIndex <= rf.lastIncludedIndex {
			// current leader does not have enough log to sync the outdated peer,
			// because logs were cleared after the snapshot, then send an InstallSnapshot RPC instead
			rf.sendSnapshot(peer)
			continue
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.getEntry(nextIndex - 1).Term,
			LeaderCommit: rf.commitIndex,
		}
		if lastLogIndex >= nextIndex {
			// If last log index ≥ nextIndex for a follower:
			// send AppendEntries RPC with log entries starting at nextIndex
			args.Entries = rf.getSlice(nextIndex, lastLogIndex+1)
			Debug(dLog, "S%d -> S%d Sending append entries at T%d. PLI: %d, PLT: %d, LC: %d. Entries: %v.",
				rf.me, peer, rf.currentTerm, args.PrevLogIndex,
				args.PrevLogTerm, args.LeaderCommit, args.Entries,
			)
			go rf.leaderSendEntries(args, peer)
		} else if isHeartbeat {
			args.Entries = make([]LogEntry, 0)
			Debug(dLog, "S%d -> S%d Sending heartbeat at T%d. PLI: %d, PLT: %d, LC: %d.",
				rf.me, peer, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
			go rf.leaderSendEntries(args, peer)
		}
	}
}

func (rf *Raft) leaderSendEntries(args *AppendEntriesArgs, server int) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		Debug(dLog, "S%d <- S%d Received send entry reply at T%d.", rf.me, server, rf.currentTerm)
		if reply.Term < rf.currentTerm {
			Debug(dLog, "S%d Term lower, invalid send entry reply. (%d < %d)",
				rf.me, reply.Term, rf.currentTerm)
			return
		}
		if rf.currentTerm != args.Term {
			Debug(dWarn, "S%d Term has changed after the append request, send entry reply discarded. "+
				"requestTerm: %d, currentTerm: %d.", rf.me, args.Term, rf.currentTerm)
			return
		}
		if rf.checkTerm(reply.Term) {
			return
		}
		// If successful: update nextIndex and matchIndex for follower (§5.3)
		if reply.Success {
			Debug(dLog, "S%d <- S%d Log entries in sync at T%d.", rf.me, server, rf.currentTerm)
			newNext := args.PrevLogIndex + 1 + len(args.Entries)
			newMatch := args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = max(newNext, rf.nextIndex[server])
			rf.matchIndex[server] = max(newMatch, rf.matchIndex[server])
			// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
			// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
			for N := rf.lastIncludedIndex + len(rf.log); N > rf.commitIndex && rf.getEntry(N).Term == rf.currentTerm; N-- {
				count := 1
				for peer, matchIndex := range rf.matchIndex {
					if peer == rf.me {
						continue
					}
					if matchIndex >= N {
						count++
					}
				}
				if count > len(rf.peers)/2 {
					rf.commitIndex = N
					Debug(dCommit, "S%d Updated commitIndex at T%d for majority consensus. CI: %d.", rf.me, rf.currentTerm, rf.commitIndex)
					break
				}
			}
		} else {
			// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
			// the optimization that backs up nextIndex by more than one entry at a time
			if reply.XTerm == -1 {
				// follower's log is too short
				rf.nextIndex[server] = reply.XLen + 1
			} else {
				_, maxIndex := rf.getBoundsWithTerm(reply.XTerm)
				if maxIndex != -1 {
					// leader has XTerm
					rf.nextIndex[server] = maxIndex
				} else {
					// leader doesn't have XTerm
					rf.nextIndex[server] = reply.XIndex
				}
			}
			lastLogIndex, _ := rf.lastLogInfo()
			nextIndex := rf.nextIndex[server]
			if nextIndex <= rf.lastIncludedIndex {
				// current leader does not have enough log to sync the outdated peer,
				// because logs were cleared after the snapshot, then send an InstallSnapshot RPC instead
				rf.sendSnapshot(server)
			} else if lastLogIndex >= nextIndex {
				Debug(dLog, "S%d <- S%d Inconsistent logs, retrying.", rf.me, server)
				newArg := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: nextIndex - 1,
					PrevLogTerm:  rf.getEntry(nextIndex - 1).Term,
					Entries:      rf.getSlice(nextIndex, lastLogIndex+1),
				}
				go rf.leaderSendEntries(newArg, server)
			}
		}
	}
}
