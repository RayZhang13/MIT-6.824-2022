package raft

import "sync"

// RequestVoteArgs is the RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

// RequestVoteReply is the RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// code to send a RequestVote RPC to a server.
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
// handler function on the server side does not return.  Thus, there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dVote, "S%d <- S%d Received vote request at T%d.", rf.me, args.CandidateId, rf.currentTerm)
	reply.VoteGranted = false

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		Debug(dVote, "S%d Term is lower, rejecting the vote. (%d < %d)", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}

	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm

	// If votedFor is null or candidateId,
	// and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	lastLogIndex, lastLogTerm := rf.log.lastLogInfo()
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if lastLogTerm < args.LastLogTerm ||
			(lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			Debug(dVote, "S%d Granting vote to S%d at T%d.", rf.me, args.CandidateId, args.Term)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
			Debug(dTimer, "S%d Resetting ELT, wait for next potential election timeout.", rf.me)
			rf.setElectionTimeout(randElectionTimeout())
		} else {
			Debug(dVote, "S%d Candidate's log not up-to-date, rejecting the vote. LLI: %d, %d. LLT: %d, %d.",
				rf.me, lastLogIndex, args.LastLogIndex, lastLogTerm, args.LastLogTerm)
		}
	} else {
		Debug(dVote, "S%d Already voted for S%d, rejecting the vote.", rf.me, rf.votedFor)
	}
}

func (rf *Raft) raiseElection() {
	rf.state = CandidateState
	// Increment currentTerm
	rf.currentTerm++
	Debug(dTerm, "S%d Starting a new term. Now at T%d.", rf.me, rf.currentTerm)
	// Vote for self
	rf.votedFor = rf.me
	rf.persist()
	// Reset election timer
	rf.setElectionTimeout(randElectionTimeout())
	Debug(dTimer, "S%d Resetting ELT because of election, wait for next potential election timeout.", rf.me)
	lastLogIndex, lastLogTerm := rf.log.lastLogInfo()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	voteCount := 1
	var once sync.Once
	// Send RequestVote RPCs to all other servers
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		Debug(dVote, "S%d -> S%d Sending request vote at T%d.", rf.me, peer, rf.currentTerm)
		go rf.candidateRequestVote(&voteCount, args, &once, peer)
	}
}

func (rf *Raft) candidateRequestVote(voteCount *int, args *RequestVoteArgs, once *sync.Once, server int) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		Debug(dVote, "S%d <- S%d Received request vote reply at T%d.", rf.me, server, rf.currentTerm)
		if reply.Term < rf.currentTerm {
			Debug(dVote, "S%d Term is lower, invalid vote reply. (%d < %d)", rf.me, reply.Term, rf.currentTerm)
			return
		}
		// If AppendEntries RPC received from new leader: convert to follower
		rf.checkTerm(reply.Term)
		if rf.currentTerm != args.Term {
			Debug(dWarn, "S%d Term has changed after the vote request, vote reply discarded."+
				"requestTerm: %d, currentTerm: %d.", rf.me, args.Term, rf.currentTerm)
			return
		}
		if reply.VoteGranted {
			*voteCount++
			Debug(dVote, "S%d <- S%d Get a yes vote at T%d.", rf.me, server, rf.currentTerm)
			// If votes received from the majority of servers: become leader
			if *voteCount > len(rf.peers)/2 {
				once.Do(func() {
					Debug(dLeader, "S%d Received majority votes at T%d. Become leader.", rf.me, rf.currentTerm)
					rf.state = LeaderState
					lastLogIndex, _ := rf.log.lastLogInfo()
					for peer := range rf.peers {
						rf.nextIndex[peer] = lastLogIndex + 1
						rf.matchIndex[peer] = 0
					}
					// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
					rf.sendEntries(true)
				})
			}
		} else {
			Debug(dVote, "S%d <- S%d Get a no vote at T%d.", rf.me, server, rf.currentTerm)
		}
	}
}
