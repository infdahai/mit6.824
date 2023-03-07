package raft

import (
	"bytes"
	"log"

	"6.824/labgob"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var rlog []LogEntry
	var lastSSIndex int
	var lastSSTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&rlog) != nil ||
		d.Decode(&lastSSIndex) != nil ||
		d.Decode(&lastSSTerm) != nil {
		log.Fatal("failed to read persist\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = rlog
		rf.lastSnapshotIndex = lastSSIndex
		rf.lastSnapshotTerm = lastSSTerm
	}
}
