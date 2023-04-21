package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

type State int

const (
	FollowerState State = iota
	CandidateState
	LeaderState
)

// Raft is a Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	leaderId      int // the leader server that current server recognizes
	state         State
	heartbeatTime time.Time
	electionTime  time.Time

	// Persistent state on all servers

	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader

	// Volatile state on all servers

	commitIndex int           // index of highest log entry known to be committed
	lastApplied int           // index of highest log entry applied to state machine
	applyCh     chan ApplyMsg // the channel on which the tester or service expects Raft to send ApplyMsg messages

	// Volatile state on leaders (Reinitialized after election)

	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	// Snapshot state on all servers
	lastIncludedIndex int    // the snapshot replaces all entries up through and including this index, entire log up to the index discarded
	lastIncludedTerm  int    // term of lastIncludedIndex
	snapshot          []byte // snapshot stored in memory

	// Temporary location to give the service snapshot to the apply thread
	// All apply messages should be sent in one go routine, we need the temporary space for applyLogsLoop to handle the snapshot apply
	waitingIndex    int    // lastIncludedIndex to be sent to applyCh
	waitingTerm     int    // lastIncludedTerm to be sent to applyCh
	waitingSnapshot []byte // snapshot to be sent to applyCh
}

// GetState returns currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LeaderState
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	Debug(dPersist, "S%d Saving persistent state to stable storage at T%d.", rf.me, rf.currentTerm)
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.currentTerm\". err: %v, data: %v", err, rf.currentTerm)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.votedFor\". err: %v, data: %v", err, rf.votedFor)
	}
	if err := e.Encode(rf.log); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.log\". err: %v, data: %v", err, rf.log)
	}
	if err := e.Encode(rf.lastIncludedIndex); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.lastIncludedIndex\". err: %v, data: %v", err, rf.lastIncludedIndex)
	}
	if err := e.Encode(rf.lastIncludedTerm); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.lastIncludedTerm\". err: %v, data: %v", err, rf.lastIncludedTerm)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// save Raft's persistent state and service snapshot to stable storage,
// where they can later be retrieved after a crash and restart.
func (rf *Raft) persistAndSnapshot(snapshot []byte) {
	Debug(dSnap, "S%d Saving persistent state and service snapshot to stable storage at T%d.", rf.me, rf.currentTerm)
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		Debug(dError, "Raft.persistAndSnapshot: failed to encode \"rf.currentTerm\". err: %v, data: %v", err, rf.currentTerm)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		Debug(dError, "Raft.persistAndSnapshot: failed to encode \"rf.votedFor\". err: %v, data: %v", err, rf.votedFor)
	}
	if err := e.Encode(rf.log); err != nil {
		Debug(dError, "Raft.persistAndSnapshot: failed to encode \"rf.log\". err: %v, data: %v", err, rf.log)
	}
	if err := e.Encode(rf.lastIncludedIndex); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.lastIncludedIndex\". err: %v, data: %v", err, rf.lastIncludedIndex)
	}
	if err := e.Encode(rf.lastIncludedTerm); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.lastIncludedTerm\". err: %v, data: %v", err, rf.lastIncludedTerm)
	}
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	Debug(dPersist, "S%d Restoring previously persisted state at T%d.", rf.me, rf.currentTerm)
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if err := d.Decode(&rf.currentTerm); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.currentTerm\". err: %v, data: %s", err, data)
	}
	if err := d.Decode(&rf.votedFor); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.votedFor\". err: %v, data: %s", err, data)
	}
	if err := d.Decode(&rf.log); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.log\". err: %v, data: %s", err, data)
	}
	if err := d.Decode(&rf.lastIncludedIndex); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.lastIncludedIndex\". err: %v, data: %s", err, data)
	}
	if err := d.Decode(&rf.lastIncludedTerm); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.lastIncludedTerm\". err: %v, data: %s", err, data)
	}
}

// CondInstallSnapshot returns if the service wants to switch to snapshot.
// Only do so if Raft hasn't had more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dSnap, "S%d Installing the snapshot. LLI: %d, LLT: %d", rf.me, lastIncludedIndex, lastIncludedTerm)
	lastLogIndex, _ := rf.lastLogInfo()
	if rf.commitIndex >= lastIncludedIndex {
		Debug(dSnap, "S%d Log entries is already up-to-date with the snapshot. (%d >= %d)", rf.me, rf.commitIndex, lastIncludedIndex)
		return false
	}
	if lastLogIndex >= lastIncludedIndex {
		rf.log = rf.getSlice(lastIncludedIndex+1, lastLogIndex+1)
	} else {
		rf.log = []LogEntry{}
	}
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.snapshot = snapshot
	rf.persistAndSnapshot(snapshot)
	return true
}

// Snapshot is the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	Debug(dSnap, "S%d Snapshotting through index %d.", rf.me, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogIndex, _ := rf.lastLogInfo()
	if rf.lastIncludedIndex >= index {
		Debug(dSnap, "S%d Snapshot already applied to persistent storage. (%d >= %d)", rf.me, rf.lastIncludedIndex, index)
		return
	}
	if rf.commitIndex < index {
		Debug(dWarn, "S%d Cannot snapshot uncommitted log entries, discard the call. (%d < %d)", rf.me, rf.commitIndex, index)
		return
	}
	newLog := rf.getSlice(index+1, lastLogIndex+1)
	newLastIncludeTerm := rf.getEntry(index).Term

	rf.lastIncludedTerm = newLastIncludeTerm
	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.snapshot = snapshot
	rf.persistAndSnapshot(snapshot)
}

// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
// Check if current term is out of date when hearing from other peers,
// update term, revert to follower state and return true if necessary
func (rf *Raft) checkTerm(term int) bool {
	if rf.currentTerm < term {
		Debug(dTerm, "S%d Term is higher, updating term to T%d, setting state to follower. (%d > %d)",
			rf.me, term, term, rf.currentTerm)
		rf.state = FollowerState
		rf.currentTerm = term
		rf.votedFor = -1
		rf.leaderId = -1
		rf.persist()
		return true
	}
	return false
}

// Start is the service using Raft (e.g. a k/v server) wants to start
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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = -1
	term = -1

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LeaderState {
		return -1, -1, false
	}
	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, logEntry)
	index = len(rf.log) + rf.lastIncludedIndex
	term = rf.currentTerm
	Debug(dLog, "S%d Add command at T%d. LI: %d, Command: %v\n", rf.me, term, index, command)
	rf.persist()
	rf.sendEntries(false)

	return index, term, true
}

// Kill causes the current Raft peer to stop.
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	Debug(dClient, "S%d Current client is exiting.", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Make creates a Raft server for the service or tester. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.leaderId = -1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = FollowerState
	rf.setElectionTimeout(randHeartbeatTimeout())
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for peer := range rf.peers {
		rf.nextIndex[peer] = 1
	}
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	lastLogIndex, lastLogTerm := rf.lastLogInfo()
	Debug(dClient, "S%d Started at T%d. LLI: %d, LLT: %d.", rf.me, rf.currentTerm, lastLogIndex, lastLogTerm)

	// start ticker goroutine to start elections
	go rf.ticker()

	// Apply logs periodically until the last committed index to make sure state machine is up-to-date.
	go rf.applyLogsLoop()

	return rf
}
