package raft

import (
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) applier() {
	for !rf.killed() {
		time.Sleep(AppliedTimeout * time.Millisecond)

		rf.mu.Lock()
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		message := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
			rf.lastApplied += 1
			message = append(message, ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				CommandIndex:  rf.lastApplied,
				Command:       rf.getLogWithIndex(rf.lastApplied).Command,
			})
		}
		rf.mu.Unlock()

		rf.DPrintf(dwarn, dcommit, "apply rf[%d]=%+v  current log: %s", rf.lastApplied, message, rf.FormatLog())

		for _, mes := range message {
			rf.applyCh <- mes
		}

	}
}

func (rf *Raft) updateCommitInd(nodeState StateType, leaderCommit int) {
	if nodeState != StateLeader {
		if leaderCommit > rf.commitIndex {
			rf.commitIndex = min(rf.getLastIndex(), leaderCommit)
		}
		DPrintf("[CommitIndex] Fllower %d commitIndex %d", rf.me, rf.commitIndex)
		return
	}

	if nodeState == StateLeader {
		rf.commitIndex = max(rf.commitIndex, rf.lastSnapshotIndex)

		for index := rf.getLastIndex(); index >= rf.commitIndex+1; index-- {
			cnt := 0
			for i := range rf.peers {
				if i == rf.me {
					cnt += 1
				} else if rf.matchIndex[i] >= index {
					cnt += 1
				}
			}

			if rf.IsMajority(cnt) && rf.getLogTermWithIndex(index) == rf.currentTerm {
				rf.commitIndex = index
				break
			}
		}

		DPrintf("[CommitIndex] Leader %d(term%d) commitIndex %d",
			rf.me, rf.currentTerm, rf.commitIndex)
		return
	}

}
