package raft

import "time"

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // with leaderId follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
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

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogTerm == -1 || args.PrevLogTerm != rf.log.getEntry(args.PrevLogIndex).Term {
		Debug(dLog2, "S%d Prev log entries do not match. Ask leader to retry.", rf.me)
		return
	}
	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	for i, entry := range args.Entries {
		if rf.log.getEntry(i+1+args.PrevLogIndex).Term != entry.Term {
			rf.log = append(rf.log.getSlice(1, i+1+args.PrevLogIndex), args.Entries[i:]...)
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		Debug(dCommit, "S%d Get higher LC at T%d, updating commitIndex. (%d < %d)",
			rf.me, rf.currentTerm, rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		Debug(dCommit, "S%d Updated commitIndex at T%d. CI: %d.", rf.me, rf.currentTerm, rf.commitIndex)
	}
	Debug(dLog2, "S%d <- S%d Append entries success. Saved logs: %v.", rf.me, args.LeaderId, args.Entries)
	reply.Success = true
}

func (rf *Raft) sendEntries(isHeartbeat bool) {
	Debug(dTimer, "S%d Resetting HBT, wait for next heartbeat broadcast.", rf.me)
	rf.setHeartbeatTimeout(time.Duration(HeartbeatInterval) * time.Millisecond)
	Debug(dTimer, "S%d Resetting ELT, wait for next potential heartbeat timeout.", rf.me)
	rf.setElectionTimeout(randHeartbeatTimeout())
	lastLogIndex, _ := rf.log.lastLogInfo()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		nextIndex := rf.nextIndex[peer]
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.log.getEntry(nextIndex - 1).Term,
			LeaderCommit: rf.commitIndex,
		}
		if isHeartbeat {
			args.Entries = make([]LogEntry, 0)
			Debug(dLog, "S%d -> S%d Sending heartbeat at T%d. PLI: %d, PLT: %d, LC: %d.",
				rf.me, peer, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
			go rf.leaderSendEntries(args, peer)
		} else if lastLogIndex >= nextIndex {
			// If last log index ≥ nextIndex for a follower:
			// send AppendEntries RPC with log entries starting at nextIndex
			entries := make([]LogEntry, lastLogIndex-nextIndex+1)
			copy(entries, rf.log.getSlice(nextIndex, lastLogIndex+1))
			args.Entries = entries
			Debug(dLog, "S%d -> S%d Sending append entries at T%d. PLI: %d, PLT: %d, LC: %d. Entries: %v.",
				rf.me, peer, rf.currentTerm, args.PrevLogIndex,
				args.PrevLogTerm, args.LeaderCommit, args.Entries,
			)
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
		if rf.checkTerm(reply.Term) {
			return
		}
		if rf.currentTerm == args.Term {
			// If successful: update nextIndex and matchIndex for follower (§5.3)
			if reply.Success {
				Debug(dLog, "S%d <- S%d Log entries in sync at T%d.", rf.me, server, rf.currentTerm)
				newNext := args.PrevLogIndex + 1 + len(args.Entries)
				newMatch := args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = max(newNext, rf.nextIndex[server])
				rf.matchIndex[server] = max(newMatch, rf.matchIndex[server])
				// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
				// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
				for N := len(rf.log); N > rf.commitIndex && rf.log.getEntry(N).Term == rf.currentTerm; N-- {
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
				if rf.nextIndex[server] > 1 {
					rf.nextIndex[server]--
				}
				lastLogIndex, _ := rf.log.lastLogInfo()
				nextIndex := rf.nextIndex[server]
				if lastLogIndex >= nextIndex {
					Debug(dLog, "S%d <- S%d Inconsistent logs, retrying.", rf.me, server)
					entries := make([]LogEntry, lastLogIndex-nextIndex+1)
					copy(entries, rf.log.getSlice(nextIndex, lastLogIndex+1))
					newArg := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: nextIndex - 1,
						PrevLogTerm:  rf.log.getEntry(nextIndex - 1).Term,
						Entries:      entries,
					}
					go rf.leaderSendEntries(newArg, server)
				}
			}
		}
	}
}
