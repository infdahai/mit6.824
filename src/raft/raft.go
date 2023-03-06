package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type StateType int8

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"Follower",
	"Candidate",
	"Leader",
}

func (rf *Raft) String() string {
	return stmap[rf.state]
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state       StateType
	currentTerm int // latest term server has seen
	// (initialized to 0 on first boot, increases monotonically)
	votedFor int // candidateId that received vote in current term (or null if none)
	log      []LogEntry

	commitIndex int // index of highest log entry known to be committed
	// initialized to 0, increases monotonically
	lastApplied int // index of highest log entry applied to state machine
	// (initialized to 0, increases monotonically)
	nextIndex []int // for each server, index of the next log entry to send to that server
	// (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server
	// (initialized to 0, increases monotonically)

	applyCh chan ApplyMsg

	// applyCond     *sync.Cond
	// replicateCond []*sync.Cond

	lastResetElectionTime time.Time

	lastSnapshotIndex int
	lastSnapshotTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.currentTerm, rf.state == StateLeader
}

func (rf *Raft) Me() int {
	return rf.me
}

//
// the service using Raft (e.g. a k/v server) wants to start
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return -1, -1, false
	}
	if rf.state != StateLeader {
		return -1, -1, false
	}

	index := rf.getLastIndex() + 1
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	rf.persist()
	return index, term, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

const (
	HeartbeatInterval  = 25
	ElectionTimeoutMax = 100
	ElectionTimeoutMin = 50
	AppliedTimeout     = 27
)

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
	}

	rf.mu.Lock()
	rf.state = StateFollower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.lastSnapshotIndex = 0
	rf.lastSnapshotTerm = 0
	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{})
	rf.applyCh = applyCh
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.lastSnapshotIndex > 0 {
		rf.lastApplied = rf.lastSnapshotIndex
	}

	rf.DPrintf(dinfo, dlog, "Make server:%s", rf.FormatState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartBeater()
	go rf.applier()

	return rf
}
