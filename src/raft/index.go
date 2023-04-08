package raft

import (
	"log"
	"math"
)

type LogEntries []LogEntry

// LogEntry is a Go object implementing raft log entry
type LogEntry struct {
	Command interface{} // Command to be excuted
	Term    int         // Term number when created
}

// Get the entry with the given index.
// Index and term of *valid* entries start from 1.
// If no *valid* entry is found, return an empty entry with term equal to -1.
func (logEntries LogEntries) getEntry(index int) *LogEntry {
	if index < 0 {
		log.Panic("LogEntries.getEntry: index < 0.\n")
	}
	if index == 0 {
		return &LogEntry{
			Command: nil,
			Term:    0,
		}
	}
	if index > len(logEntries) {
		return &LogEntry{
			Command: nil,
			Term:    -1,
		}
	}
	return &logEntries[index-1]
}

// Get the index and term of the last entry.
// Return (0, 0) if the log is empty.
func (logEntries LogEntries) lastLogInfo() (index, term int) {
	index = len(logEntries)
	logEntry := logEntries.getEntry(index)
	return index, logEntry.Term
}

// Get the slice of the log with index from startIndex to endIndex.
// startIndex included and endIndex excluded, therefore startIndex should be no greater than endIndex.
func (logEntries LogEntries) getSlice(startIndex, endIndex int) LogEntries {
	if startIndex <= 0 {
		Debug(dError, "LogEntries.getSlice: startIndex out of range. startIndex: %d, len: %d.",
			startIndex, len(logEntries))
		log.Panic("LogEntries.getSlice: startIndex out of range. \n")
	}
	if endIndex > len(logEntries)+1 {
		Debug(dError, "LogEntries.getSlice: endIndex out of range. endIndex: %d, len: %d.",
			endIndex, len(logEntries))
		log.Panic("LogEntries.getSlice: endIndex out of range.\n")
	}
	if startIndex > endIndex {
		Debug(dError, "LogEntries.getSlice: startIndex > endIndex. (%d > %d)", startIndex, endIndex)
		log.Panic("LogEntries.getSlice: startIndex > endIndex.\n")
	}
	return logEntries[startIndex-1 : endIndex-1]
}

// Get the index of first entry and last entry with the given term.
// Return (-1,-1) if no such term is found
func (logEntries LogEntries) getBoundsWithTerm(term int) (minIndex int, maxIndex int) {
	if term == 0 {
		return 0, 0
	}
	minIndex, maxIndex = math.MaxInt, -1
	for i := 1; i <= len(logEntries); i++ {
		if logEntries.getEntry(i).Term == term {
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
