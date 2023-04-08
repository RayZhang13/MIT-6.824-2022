package raft

import (
	"math/rand"
	"time"
)

// These constants are all in milliseconds.
const (
	TickInterval         int64 = 30   // Loop interval for ticker and applyLogsLoop. Mostly for checking timeouts.
	HeartbeatInterval    int64 = 100  // Interval between heartbeats. Broadcast heartbeats when timeout as a leader.
	BaseHeartbeatTimeout int64 = 300  // Lower bound of heartbeat timeout. Election is raised when timeout as a follower.
	BaseElectionTimeout  int64 = 1000 // Lower bound of election timeout. Another election is raised when timeout as a candidate.
)

const RandomFactor float64 = 0.8 // Factor to control upper bound of heartbeat timeouts and election timeouts.

func (rf *Raft) setElectionTimeout(timeout time.Duration) {
	t := time.Now()
	t = t.Add(timeout)
	rf.electionTime = t
}

func (rf *Raft) setHeartbeatTimeout(timeout time.Duration) {
	t := time.Now()
	t = t.Add(timeout)
	rf.heartbeatTime = t
}

func randElectionTimeout() time.Duration {
	extraTime := int64(float64(rand.Int63()%BaseElectionTimeout) * RandomFactor)
	return time.Duration(extraTime+BaseElectionTimeout) * time.Millisecond
}

func randHeartbeatTimeout() time.Duration {
	extraTime := int64(float64(rand.Int63()%BaseHeartbeatTimeout) * RandomFactor)
	return time.Duration(extraTime+BaseHeartbeatTimeout) * time.Millisecond
}
