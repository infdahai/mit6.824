package raft

type LogEntry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) getLastIndex() int {
	return len(rf.log) + rf.lastSnapshotIndex - 1
}

func (rf *Raft) getLastTerm() int {
	if len(rf.log) == 1 {
		return rf.lastSnapshotTerm
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

func (rf *Raft) getLogWithIndex(globalInd int) LogEntry {
	return rf.log[globalInd-rf.lastSnapshotIndex]
}

func (rf *Raft) getLogTermWithIndex(globalInd int) int {
	// TODO(infdahai): assert(globalInd>=lastSSIndex)
	if globalInd == rf.lastSnapshotIndex {
		return rf.lastSnapshotTerm
	}
	return rf.log[globalInd-rf.lastSnapshotIndex].Term
}

func (rf *Raft) getPrevLogInfo(peer int) (int, int) {
	prevLogInd := rf.nextIndex[peer] - 1
	lastInd := rf.getLastIndex()
	// TODO(infdahai): ????
	if prevLogInd >= lastInd+1 {
		prevLogInd = lastInd
	}
	return prevLogInd, rf.getLogTermWithIndex(prevLogInd)
}

func (rf *Raft) isLogUpToDate(args *RequestVoteArgs) bool {
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	return (args.LastLogTerm > lastTerm) ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)
}
