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
	//	"bytes"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

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
	log      RaftLog

	commitIndex int // index of highest log entry known to be committed
	// initialized to 0, increases monotonically
	lastApplied int // index of highest log entry applied to state machine
	// (initialized to 0, increases monotonically)
	nextIndex []int // for each server, index of the next log entry to send to that server
	// (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server
	// (initialized to 0, increases monotonically)

	applyCh       chan ApplyMsg
	applyCond     *sync.Cond
	replicateCond []*sync.Cond

	// electionTime  time.Duration
	// lastHeartBeat time.Time
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.DPrintf(dwarn, dlock, "GetState: [R] acq mu.lock")
	rf.mu.RLock()
	rf.DPrintf(dlock2, dlock, "GetState: [R] get mu.lock")

	defer rf.DPrintf(ddebug, dlock, "GetState: [R] release mu.lock")
	defer rf.mu.RUnlock()

	term := rf.currentTerm
	isleader := rf.state == StateLeader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	var rlog RaftLog
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&rlog) != nil {
		log.Fatal("failed to read persist\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = rlog
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) resetTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
}

func (rf *Raft) changeState(ns StateType) {
	rf.state = ns
}

// lock_guard
func (rf *Raft) isLogUpToDate(args *RequestVoteArgs) bool {
	lastIndex := rf.log.lastLog().Index
	lastTerm := rf.log.lastLog().Term
	return (args.LastLogTerm > lastTerm) || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)
}

//TODO(infdahai): remember lastHeartBeat is setting on grant successful votes and reply for appendEntry.
//
// Invoked by candidates to gather votes.
//
// then args are processed by candidate.
// reply is processed by server.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.DPrintf(dwarn, dlock, "RequestVote: acq mu.lock")
	rf.mu.Lock()
	rf.DPrintf(dlock2, dlock, "RequestVote: get mu.lock")

	defer rf.DPrintf(ddebug, dlock, "RequestVote: release mu.lock")
	defer rf.mu.Unlock()

	defer rf.persist()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.changeState(StateFollower)
		rf.resetTerm(args.Term)
		reply.Term = args.Term
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isLogUpToDate(args) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.electionTimer.Reset(RandomizedElectionTime())
	} else {
		reply.VoteGranted = false
	}
}

// lock_guard
func (rf *Raft) matchlog(prevLogIndex int, prevLogTerm int) bool {
	if rf.log.len() > prevLogIndex {
		return rf.log.at(prevLogIndex).Index == prevLogIndex && rf.log.at(prevLogIndex).Term == prevLogTerm
	}
	return false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.DPrintf(dwarn, dlock, "AppendEntries: acq mu.lock")
	rf.mu.Lock()
	rf.DPrintf(dlock2, dlock, "AppendEntries: get mu.lock")

	defer rf.DPrintf(ddebug, dlock, "AppendEntries: release mu.lock")
	defer rf.mu.Unlock()

	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// TODO: rpc call will initialize lastHeartBeat

	rf.changeState(StateFollower)
	if args.Term > rf.currentTerm {
		rf.resetTerm(args.Term)
	}
	rf.electionTimer.Reset(RandomizedElectionTime())

	if !rf.matchlog(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		lastIndex := rf.log.lastLog().Index

		// Important:
		// XIndex will be equal to (nextIndex - 1)
		if lastIndex < args.PrevLogIndex {
			reply.XTerm = -1
			reply.XIndex = lastIndex + 1
		} else {
			// change log at the current term
			xTerm := rf.log.at(args.PrevLogIndex).Term
			for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
				if rf.log.at(xIndex-1).Term != xTerm {
					reply.XIndex = xIndex
					break
				}
			}
		}
		return
	}

	// TODO: check check
	// match case
	if len(args.Entries) > 0 {
		for index, entry := range args.Entries {
			// FIXME: when entry.Index>rf.log.len+1 => cause exception
			if entry.Index >= rf.log.len() || rf.log.at(entry.Index).Term != entry.Term {
				rf.log.Entries = append(rf.log.Entries[:entry.Index], args.Entries[index:]...)
				break
			}
		}
	}
	rf.log.at(0).Index = rf.log.lastLog().Index
	// keep log[0].index == lastlog.index

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastLog().Index)
		rf.applyCond.Broadcast()
		// wake up applier(0)
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

//
// example code to send a RequestVote RPC to a server.
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
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) genRequestVoteRequest() *RequestVoteArgs {
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.lastLog().Index,
		LastLogTerm:  rf.log.lastLog().Term,
	}
	return args
}

func (rf *Raft) genAppendEntriesRequest(peer int) *AppendEntriesArgs {
	nextIndex := rf.nextIndex[peer]
	entries := make([]LogEntry, rf.log.len()-nextIndex)
	for j := nextIndex; j < rf.log.len(); j++ {
		entries = append(entries, *rf.log.at(j))
	}

	prevLogIndex := rf.log.at(nextIndex - 1).Index
	prevLogTerm := rf.log.at(nextIndex - 1).Term

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

// lock_guard
// TODO(infdahai): maybe need persist()
func (rf *Raft) appendNewEntry(command interface{}) *LogEntry {
	defer rf.persist()
	rf.log.at(0).Index += 1
	rf.log.append(rf.log.at(0).Index, rf.currentTerm, command)
	return rf.log.lastLog()
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
	rf.DPrintf(dwarn, dlock, "Start: acq mu.lock")
	rf.mu.Lock()
	rf.DPrintf(dlock2, dlock, "Start: get mu.lock")

	defer rf.DPrintf(ddebug, dlock, "Start: release mu.lock")
	defer rf.mu.Unlock()

	if rf.state != StateLeader {
		return -1, -1, false
	}
	newLog := rf.appendNewEntry(command)
	rf.matchIndex[rf.me] += 1

	rf.DPrintf(dinfo, dclient, "client start replica with log %s", rf.FormatState())

	rf.BroadcastHeartbeat(false)
	return newLog.Index, newLog.Term, rf.state == StateLeader
}

func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			go rf.Replicate(peer)
			// just call goroutine replicate()  (it exists risk.)
		} else {
			rf.replicateCond[peer].Signal()
			// call replicate() func immediately
			// from start
		}
	}
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int // Term in prevLogIndex
	XIndex  int // first index in XTerm
}

func (rf *Raft) IsMajority(num int) bool {
	return (num >= (len(rf.peers)/2 + 1))
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.DPrintf(dwarn, dlock, "ticker(elect): acq mu.lock")
			rf.mu.Lock()
			rf.DPrintf(dlock2, dlock, "ticker(elect): get mu.lock")

			rf.StartElection()
			rf.electionTimer.Reset(RandomizedElectionTime())

			rf.mu.Unlock()
			rf.DPrintf(ddebug, dlock, "ticker(elect): release mu.lock")
		case <-rf.heartbeatTimer.C:
			rf.DPrintf(dwarn, dlock, "ticker(heart): acq mu.lock")
			rf.mu.Lock()
			rf.DPrintf(dlock2, dlock, "ticker(heart): get mu.lock")

			if rf.state == StateLeader {
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}

			rf.mu.Unlock()
			rf.DPrintf(ddebug, dlock, "ticker(heart): release mu.lock")
		}
	}
}

// lock_gurad
func (rf *Raft) StartElection() {
	rf.currentTerm += 1
	rf.changeState(StateCandidate)
	rf.votedFor = rf.me
	rf.persist()

	rf.DPrintf(dinfo, delection, "electime elpased out => Candidate")

	grantedVotes := 1

	reqArgs := rf.genRequestVoteRequest()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			var reply RequestVoteReply
			if rf.sendRequestVote(peer, reqArgs, &reply) {
				//TODO: lastHeartBeat updated.(if one server grant for votes. )

				rf.DPrintf(dwarn, dlock, "sendRequestVote: acq mu.lock")
				rf.mu.Lock()
				rf.DPrintf(dlock2, dlock, "sendRequestVote: get mu.lock")

				defer rf.DPrintf(ddebug, dlock, "sendRequestVote: release mu.lock")
				defer rf.mu.Unlock()

				// in current term
				if rf.currentTerm == reqArgs.Term && rf.state == StateCandidate {
					if reply.VoteGranted {
						grantedVotes += 1
						if rf.IsMajority(grantedVotes) {
							rf.changeState(StateLeader)

							// become a new leader
							// reinitialized after election
							for i := 0; i < len(rf.peers); i++ {
								rf.nextIndex[i] = rf.log.lastLog().Index + 1
								rf.matchIndex[i] = 0
							}
							rf.matchIndex[rf.me] = rf.log.lastLog().Index
							rf.BroadcastHeartbeat(true)
							// async replicate. don't fussy
						}
					}
				} else if rf.currentTerm < reply.Term {
					rf.changeState(StateFollower)
					rf.resetTerm(reply.Term)
					rf.persist()
				}
			}
		}(peer)
	}

	rf.DPrintf(ddebug, dlock, "ticker: release mu.lock(usually)")

}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.DPrintf(dwarn, dlock, "applier: [R] acq mu.lock(1)")
		rf.mu.Lock()

		for !rf.needApply() {
			rf.applyCond.Wait()
		}

		rf.DPrintf(dlock2, dlock, "applier: [R] get mu.lock(1)")

		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, rf.commitIndex-rf.lastApplied)
		copy(entries, rf.log.Entries[lastApplied+1:commitIndex+1])

		rf.mu.Unlock()
		rf.DPrintf(ddebug, dlock, "applier: [R] release mu.lock(1)")

		// TODO(infdahai): maybe add CommandTerm?
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}

		rf.DPrintf(dwarn, dlock, "applier: acq mu.lock(2)")
		rf.mu.Lock()
		rf.DPrintf(dlock2, dlock, "applier: get mu.lock(2)")

		rf.lastApplied = max(rf.lastApplied, commitIndex)

		rf.mu.Unlock()
		rf.DPrintf(ddebug, dlock, "applier: release mu.lock(2)")
	}
}

func (rf *Raft) needApply() bool {
	rf.DPrintf(dinfo, dtrace, "needApply: commitInd=%d, lastApplied=%d", rf.commitIndex, rf.lastApplied)
	return rf.commitIndex > rf.lastApplied
}

// TODO(infdahai): decide whether peer != rf.me(in replicator(peer) and Replicate(peer) func).
// I think it doesn't matter.
func (rf *Raft) needReplicate(peer int) bool {
	rf.DPrintf(dwarn, dlock, "needReplicate: [R] acq mu.lock")
	rf.mu.RLock()
	rf.DPrintf(dlock2, dlock, "needReplicate: [R] get mu.lock")

	defer rf.DPrintf(ddebug, dlock, "needReplicate: [R] release mu.lock")
	defer rf.mu.RUnlock()

	rf.DPrintf(dinfo, dtrace, "needReplicate: id:%d, matchInd:%d, lastLogInd:%d", peer, rf.matchIndex[peer], rf.log.lastLog().Index)
	return rf.state == StateLeader && rf.matchIndex[peer] < rf.log.lastLog().Index
}

func (rf *Raft) replicator(peer int) {
	rf.replicateCond[peer].L.Lock()
	defer rf.replicateCond[peer].L.Unlock()

	for !rf.killed() {
		for !rf.needReplicate(peer) {
			rf.replicateCond[peer].Wait()
		}
		rf.Replicate(peer)
	}

}

func (rf *Raft) Replicate(peer int) {
	rf.DPrintf(dwarn, dlock, "Replicate: acq mu.lock")
	rf.mu.RLock()
	rf.DPrintf(dlock2, dlock, "Replicate: get mu.lock")
	if rf.state != StateLeader {
		rf.mu.RUnlock()
		rf.DPrintf(ddebug, dlock, "Replicate: release mu.lock(1)")
		return
	}
	args := rf.genAppendEntriesRequest(peer)

	rf.mu.RUnlock()
	rf.DPrintf(ddebug, dlock, "Replicate: release mu.lock(2)")

	rf.Sync(peer, args)
}

func LogTail(a []LogEntry) *LogEntry {
	return &a[len(a)-1]
}

// lock_gurad
func (rf *Raft) Sync(peer int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	if !rf.sendAppendEntries(peer, args, &reply) {
		return
	}

	rf.DPrintf(dwarn, dlock, "Sync: acq mu.lock")
	rf.mu.Lock()
	rf.DPrintf(dlock2, dlock, "Sync: get mu.lock")

	defer rf.DPrintf(ddebug, dlock, "Sync: release mu.lock")
	defer rf.mu.Unlock()

	//

	if reply.Term > rf.currentTerm {
		rf.changeState(StateFollower)
		rf.resetTerm(reply.Term)
		rf.persist()
	}

	if reply.Success {
		if len(args.Entries) == 0 {
			return
		}
		match := args.PrevLogIndex + len(args.Entries)
		next := match + 1
		rf.matchIndex[peer] = max(rf.matchIndex[peer], match)
		rf.nextIndex[peer] = max(rf.nextIndex[peer], next)

		for n := rf.commitIndex + 1; n <= rf.log.lastLog().Index; n++ {
			if rf.log.at(n).Term != rf.currentTerm {
				continue
			}
			cnt := 0
			// include myself
			for i := range rf.peers {
				if rf.matchIndex[i] >= n {
					cnt++
				}
				//TODO(infdahai):  remember that matchindex[rf.me] needs to update.
			}
			if rf.IsMajority(cnt) {
				rf.commitIndex = n
			}
		}

		// when update commitIndex, need to try applier
		if rf.commitIndex > rf.lastApplied {
			rf.applyCond.Broadcast()
		}

	} else {
		rf.matchIndex[peer] = 0
		rf.nextIndex[peer] = reply.XIndex
	}

}

const (
	HeartbeatInterval  = int64(27 * time.Millisecond)
	ElectionTimeoutMax = int64(100 * time.Millisecond)
	ElectionTimeoutMin = int64(50 * time.Millisecond)
)

func RandomizedElectionTime() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Int63n(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin)
}

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatInterval)
}

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
		dead:      0,
		state:     StateFollower,

		currentTerm: 0,
		votedFor:    -1,

		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),

		applyCh:       applyCh,
		replicateCond: make([]*sync.Cond, len(peers)),

		// lastHeartBeat: time.Now(),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		electionTimer:  time.NewTimer(RandomizedElectionTime()),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.log.append(0, 0, nil)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	/*
	*	 go replicator.
	 */
	lastLogInd := rf.log.lastLog().Index
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLogInd+1
		if i != rf.me {
			rf.replicateCond[i] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(i)
		}
	}

	rf.DPrintf(dinfo, dlog, "Make server:%s", rf.FormatState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
