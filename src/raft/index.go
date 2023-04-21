package raft

import (
	"log"
	"math"
)

// LogEntry is a Go object implementing raft log entry
type LogEntry struct {
	Command interface{} // Command to be executed
	Term    int         // Term number when created
}

// Get the entry with the given index.
// Index and term of *valid* entries start from 1.
// If no *valid* entry is found, return an empty entry with term equal to -1.
// If log is too short, also return an empty entry with term equal to -1.
// Panic if the index of the log entry is already in the snapshot and unable to get from memory.
func (rf *Raft) getEntry(index int) *LogEntry {
	logEntries := rf.log
	logIndex := index - rf.lastIncludedIndex
	if logIndex < 0 {
		log.Panicf("LogEntries.getEntry: index too small. (%d < %d)", index, rf.lastIncludedIndex)
	}
	if logIndex == 0 {
		return &LogEntry{
			Command: nil,
			Term:    rf.lastIncludedTerm,
		}
	}
	if logIndex > len(logEntries) {
		return &LogEntry{
			Command: nil,
			Term:    -1,
		}
	}
	return &logEntries[logIndex-1]
}

// Get the index and term of the last entry.
// Return (0, 0) if the log is empty.
func (rf *Raft) lastLogInfo() (index, term int) {
	logEntries := rf.log
	index = len(logEntries) + rf.lastIncludedIndex
	logEntry := rf.getEntry(index)
	return index, logEntry.Term
}

// Get the slice of the log with index from startIndex to endIndex.
// startIndex included and endIndex excluded, therefore startIndex should be no greater than endIndex.
func (rf *Raft) getSlice(startIndex, endIndex int) []LogEntry {
	logEntries := rf.log
	logStartIndex := startIndex - rf.lastIncludedIndex
	logEndIndex := endIndex - rf.lastIncludedIndex
	if logStartIndex <= 0 {
		Debug(dError, "LogEntries.getSlice: startIndex out of range. startIndex: %d, len: %d.",
			startIndex, len(logEntries))
		log.Panicf("LogEntries.getSlice: startIndex out of range. (%d < %d)", startIndex, rf.lastIncludedIndex)
	}
	if logEndIndex > len(logEntries)+1 {
		Debug(dError, "LogEntries.getSlice: endIndex out of range. endIndex: %d, len: %d.",
			endIndex, len(logEntries))
		log.Panicf("LogEntries.getSlice: endIndex out of range. (%d > %d)", endIndex, len(logEntries)+1+rf.lastIncludedIndex)
	}
	if logStartIndex > logEndIndex {
		Debug(dError, "LogEntries.getSlice: startIndex > endIndex. (%d > %d)", startIndex, endIndex)
		log.Panicf("LogEntries.getSlice: startIndex > endIndex. (%d > %d)", startIndex, endIndex)
	}
	return append([]LogEntry(nil), logEntries[logStartIndex-1:logEndIndex-1]...)
}

// Get the index of first entry and last entry with the given term.
// Return (-1,-1) if no such term is found
func (rf *Raft) getBoundsWithTerm(term int) (minIndex int, maxIndex int) {
	logEntries := rf.log
	if term == 0 {
		return 0, 0
	}
	minIndex, maxIndex = math.MaxInt, -1
	for i := rf.lastIncludedIndex + 1; i <= rf.lastIncludedIndex+len(logEntries); i++ {
		if rf.getEntry(i).Term == term {
			minIndex = min(minIndex, i)
			maxIndex = max(maxIndex, i)
		}
	}
	if maxIndex == -1 {
		return -1, -1
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}
