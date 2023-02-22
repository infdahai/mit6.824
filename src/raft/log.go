package raft

import "fmt"

type RaftLog struct {
	Entries []LogEntry
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

func (l *RaftLog) at(idx int) *LogEntry {
	return &l.Entries[idx]
}

func (l *RaftLog) len() int {
	return len(l.Entries)
}

func (l *RaftLog) lastLog() *LogEntry {
	return l.at(l.len() - 1)
}

func (l *RaftLog) append(index int, term int, command interface{}) {
	l.Entries = append(l.Entries, LogEntry{Index: index, Term: term, Command: command})
}

// func (l *RaftLog) appendEntry(le LogEntry) {
// 	l.Entries = append(l.Entries, le)
// }

func (le LogEntry) String() string {
	return fmt.Sprintf("{%d %d %v}", le.Index, le.Term, le.Command)
}
