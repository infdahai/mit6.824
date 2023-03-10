package raft

import "time"

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) LeaderSendSnapshot(peer int) {
	rf.mu.RLock()
	DPrintf("[LeaderSendSnapShot]Leader %d (term %d) send snapshot to server %d, index %d",
		rf.me, rf.currentTerm, peer, rf.lastSnapshotIndex)

	args := rf.genInstallSnashotArgs(peer)
	rf.mu.RUnlock()
	var reply InstallSnapshotReply

	if !rf.sendSnapshot(peer, args, &reply) {
		DPrintf("[InstallSnapShot ERROR] Leader %d don't recive from %d", rf.me, peer)
		return
	}

	//=========================
	// lock release and require
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != StateLeader || rf.currentTerm != args.Term {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.ChangeState(StateFollower, true)
		return
	}

	DPrintf("[InstallSnapShot SUCCESS] Leader %d from sever %d", rf.me, peer)

	// rf.matchIndex[peer] = max(rf.matchIndex[peer], args.LastIncludedIndex)
	// rf.nextIndex[peer] = max(rf.matchIndex[peer]+1, rf.nextIndex[peer])
	// TODO(infdahai): fix 2D
	rf.matchIndex[peer] = args.LastIncludedIndex
	rf.nextIndex[peer] = args.LastIncludedIndex + 1
}

// rlock_guard
func (rf *Raft) genInstallSnashotArgs(peer int) *InstallSnapshotArgs {
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		//TODO(infdahai): fix in 2D
		Data: rf.persister.ReadSnapshot(),
	}
	return args
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastSnapshotIndex >= index || index > rf.commitIndex {
		return
	}

	// snapshot the entry(start from pos 1 )
	tempLog := make([]LogEntry, 0)
	tempLog = append(tempLog, LogEntry{})

	for i := index + 1; i <= rf.getLastIndex(); i++ {
		tempLog = append(tempLog, rf.getLogWithIndex(i))
	}

	if index >= rf.getLastIndex()+1 {
		rf.lastSnapshotTerm = rf.getLastTerm()
	} else {
		rf.lastSnapshotTerm = rf.getLogTermWithIndex(index)
	}

	rf.lastSnapshotIndex = index
	rf.log = tempLog

	rf.commitIndex = max(rf.commitIndex, index)
	rf.lastApplied = max(rf.lastApplied, index)

	DPrintf("[SnapShot]Server %d sanpshot until index %d, term %d, loglen %d", rf.me, index,
		rf.lastSnapshotTerm, len(rf.log)-1)

	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

// send by server
func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term
	if rf.state != StateFollower {
		rf.ChangeState(StateFollower, true)
	} else {
		rf.lastResetElectionTime = time.Now()
		rf.persist()
	}

	if rf.lastSnapshotIndex >= args.LastIncludedIndex {
		DPrintf("[HaveSnapShot] sever %d , lastSSPindex %d, leader's lastIncludeIndex %d",
			rf.me, rf.lastSnapshotIndex, args.LastIncludedIndex)
		rf.mu.Unlock()
		return
	}

	index := args.LastIncludedIndex
	temp := make([]LogEntry, 0)
	temp = append(temp, LogEntry{})

	for i := index + 1; i <= rf.getLastIndex(); i++ {
		temp = append(temp, rf.getLogWithIndex(i))
	}

	rf.log = temp
	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm
	rf.commitIndex = max(rf.commitIndex, index)
	rf.lastApplied = max(rf.lastApplied, index)

	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastSnapshotTerm,
		SnapshotIndex: rf.lastSnapshotIndex,
	}
	rf.mu.Unlock()

	rf.applyCh <- msg
	DPrintf("[FollowerInstallSnapShot]server %d installsnapshot from leader %d, index %d",
		rf.me, args.LeaderId, args.LastIncludedIndex)

}

func (rf *Raft) HasLogInCurrentTerm() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.getLastTerm() == rf.currentTerm
}
