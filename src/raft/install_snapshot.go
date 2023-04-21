package raft

// InstallSnapshotArgs is the InstallSnapshot RPC arguments structure.
// field names must start with capital letters!
type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
}

// InstallSnapshotReply is the InstallSnapshot RPC reply structure.
// field names must start with capital letters!
type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

// code to send an InstallSnapshot RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// InstallSnapshot sends an apply message with the snapshot to applyCh.
// The state machine should cooperate with raft code later to decide whether to install the snapshot using CondInstallSnapshot function.
// No snapshot related status in raft code should be changed right now,
// which could result in inconsistency between the status machine and raft code, as the snapshot is not applied immediately.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dSnap, "S%d <- S%d Received install snapshot request at T%d.", rf.me, args.LeaderId, rf.currentTerm)

	if args.Term < rf.currentTerm {
		Debug(dSnap, "S%d Term is lower, rejecting install snapshot request. (%d < %d)", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm

	Debug(dTimer, "S%d Resetting ELT, wait for next potential heartbeat timeout.", rf.me)
	rf.setElectionTimeout(randHeartbeatTimeout())

	// All apply messages should be sent in one go routine (applyLogsLoop),
	// otherwise the apply action could be out of order, where the snapshot apply could cut in line when the command apply is running.
	if rf.waitingIndex >= args.LastIncludedIndex {
		Debug(dSnap, "S%d A newer snapshot already exists, rejecting install snapshot request. (%d <= %d)",
			rf.me, args.LastIncludedIndex, rf.waitingIndex)
		return
	}
	rf.leaderId = args.LeaderId
	rf.waitingSnapshot = args.Data
	rf.waitingIndex = args.LastIncludedIndex
	rf.waitingTerm = args.LastIncludedTerm
}

func (rf *Raft) sendSnapshot(server int) {
	Debug(dSnap, "S%d -> S%d Sending installing snapshot request at T%d.", rf.me, server, rf.currentTerm)
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapshot,
	}
	go rf.leaderSendSnapshot(args, server)
}

func (rf *Raft) leaderSendSnapshot(args *InstallSnapshotArgs, server int) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		Debug(dSnap, "S%d <- S%d Received install snapshot reply at T%d.", rf.me, server, rf.currentTerm)
		if reply.Term < rf.currentTerm {
			Debug(dLog, "S%d Term lower, invalid install snapshot reply. (%d < %d)",
				rf.me, reply.Term, rf.currentTerm)
			return
		}
		if rf.currentTerm != args.Term {
			Debug(dWarn, "S%d Term has changed after the install snapshot request, install snapshot reply discarded. "+
				"requestTerm: %d, currentTerm: %d.", rf.me, args.Term, rf.currentTerm)
			return
		}
		rf.checkTerm(reply.Term)
		newNext := args.LastIncludedIndex + 1
		newMatch := args.LastIncludedIndex
		rf.matchIndex[server] = max(newMatch, rf.matchIndex[server])
		rf.nextIndex[server] = max(newNext, rf.nextIndex[server])
	}
}
