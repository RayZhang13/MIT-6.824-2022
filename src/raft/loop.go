package raft

import "time"

// ApplyMsg is the structure used to apply log entries to the server.
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// The ticker go routine starts a new election if this peer hasn't received heartbeats recently.
// The routine also broadcasts heartbeats and applies logs periodically.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		// leader repeat heartbeat during idle periods to prevent election timeouts (§5.2)
		if rf.state == LeaderState && time.Now().After(rf.heartbeatTime) {
			Debug(dTimer, "S%d HBT elapsed. Broadcast heartbeats.", rf.me)
			rf.sendEntries(true)
		}
		// If election timeout elapses: start new election
		if time.Now().After(rf.electionTime) {
			Debug(dTimer, "S%d ELT elapsed. Converting to Candidate, calling election.", rf.me)
			rf.raiseElection()
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(TickInterval) * time.Millisecond)
	}
}

func (rf *Raft) applyLogsLoop() {
	for !rf.killed() {
		// Apply logs periodically until the last committed index.
		rf.mu.Lock()
		// To avoid the apply operation getting blocked with the lock held,
		// use a slice to store all committed messages to apply, and apply them only after unlocked
		var appliedMsgs []ApplyMsg

		rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)

		if rf.waitingSnapshot != nil {
			appliedMsgs = append(appliedMsgs, ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.waitingSnapshot,
				SnapshotTerm:  rf.waitingTerm,
				SnapshotIndex: rf.waitingIndex,
			})
			rf.waitingSnapshot = nil
		} else {
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				appliedMsgs = append(appliedMsgs, ApplyMsg{
					CommandValid: true,
					Command:      rf.getEntry(rf.lastApplied).Command,
					CommandIndex: rf.lastApplied,
				})
				Debug(dLog2, "S%d Applying log at T%d. LA: %d, CI: %d.", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
			}
		}
		rf.mu.Unlock()
		for _, msg := range appliedMsgs {
			rf.applyCh <- msg
		}
		time.Sleep(time.Duration(TickInterval) * time.Millisecond)
	}
}
