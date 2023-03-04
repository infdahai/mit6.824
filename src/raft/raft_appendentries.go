package raft

import (
	"time"
)

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
	XIndex  int // first index in XTerm
}

// heartbeat pack by sending from leader
func (rf *Raft) LeaderAppendEntries() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		// parallel replicate logs to servers.
		go func(peer int) {
			rf.mu.RLock()
			if rf.state != StateLeader {
				rf.mu.RUnlock()
				return
			}

			prevLogIndex := rf.nextIndex[peer] - 1
			// when return lastSnapshot Index (for snapshot)
			if prevLogIndex < rf.lastSnapshotIndex {
				go rf.LeaderSendSnapshot(peer)
				rf.mu.RUnlock()
				return
			}

			reply := AppendEntriesReply{}
			args := rf.genAppendEntriesRequest(peer)
			rf.mu.RUnlock()

			if !rf.sendAppendEntries(peer, args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != StateLeader {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.DPrintf(derror, dheart, "convert to Follower(reply.Term>rf.Term)")
				rf.currentTerm = reply.Term
				rf.ChangeState(StateFollower, true)
				return
			}

			DPrintf("[HeartBeatGetReturn] Leader %d (term %d) ,from Server %d, prevLogIndex %d", rf.me, rf.currentTerm, peer, args.PrevLogIndex)

			if reply.Success {
				DPrintf("[HeartBeat SUCCESS] Leader %d (term %d) ,from Server %d, prevLogIndex %d",
					rf.me, rf.currentTerm, peer, args.PrevLogIndex)

				//TODO(indahai): do not use max func()
				rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1

				rf.updateCommitInd(StateLeader, 0)
			}

			if !reply.Success && reply.XIndex != -1 {
				rf.nextIndex[peer] = reply.XIndex
				DPrintf("[HeartBeat CONFLICT] Leader %d (term %d) ,from Server %d, prevLogIndex %d, Confilicting %d",
					rf.me, rf.currentTerm, peer, args.PrevLogIndex, reply.XIndex)
			}

		}(peer)
	}
}

//RLock_gurad
func (rf *Raft) genAppendEntriesRequest(peer int) *AppendEntriesArgs {
	var args *AppendEntriesArgs
	prevLogIndex, prevLogTerm := rf.getPrevLogInfo(peer)
	if rf.getLastIndex() >= rf.nextIndex[peer] {
		DPrintf("[LeaderAppendEntries]Leader %d (term %d) to server %d, index %d --- %d",
			rf.me, rf.currentTerm, peer, rf.nextIndex[peer], rf.getLastIndex())

		entries := make([]LogEntry, 0)
		entries = append(entries, rf.log[rf.nextIndex[peer]-rf.lastSnapshotIndex:]...)

		args = &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
	} else {
		args = &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      []LogEntry{},
			LeaderCommit: rf.commitIndex,
		}
		DPrintf("[LeaderSendHeartBeat]Leader %d (term %d) to server %d,nextIndex %d, matchIndex %d, lastIndex %d",
			rf.me, rf.currentTerm, peer, rf.nextIndex[peer], rf.matchIndex[peer], rf.getLastIndex())
	}

	rf.DPrintf(ddebug, dlog, "replicate [leader %d=>server %d] T:%d,LeaderId:%d,PrevLogInd:%d,PrevLogT:%d,LeaderCommit:%d",
		rf.me, peer, rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, rf.commitIndex)

	return args
}

func (rf *Raft) heartBeater() {
	for !rf.killed() {
		time.Sleep(HeartbeatInterval * time.Millisecond)

		rf.mu.RLock()
		if rf.state == StateLeader {
			rf.mu.RUnlock()
			rf.LeaderAppendEntries()
		} else {
			rf.mu.RUnlock()
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[GetHeartBeat]Sever %d, from Leader %d(term %d), lastInd %d, leader.prevInd %d", rf.me, args.LeaderId, args.Term, rf.getLastIndex(), args.PrevLogIndex)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XIndex = -1
		return
	}

	// TODO: rpc call will initialize lastHeartBeat
	rf.currentTerm = args.Term
	reply.Term = args.Term
	reply.Success = true
	reply.XIndex = -1

	if rf.state != StateFollower {
		rf.ChangeState(StateFollower, true)
	} else {
		rf.lastResetElectionTime = time.Now()
		rf.persist()
	}

	if rf.lastSnapshotIndex > args.PrevLogIndex {
		reply.Success = false
		reply.XIndex = rf.getLastIndex() + 1
		return
	}

	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.XIndex = rf.getLastIndex()
		// maybe snapshot, so not Adding 1
		// snapshot increase and you should grap this.

		DPrintf("[AppendEntries ERROR1]Sever %d ,prevLogIndex %d,Term %d, rf.getLastIndex %d, rf.getLastTerm %d, Confilicing %d",
			rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.getLastIndex(), rf.getLastTerm(),
			reply.XIndex)
		return
	}

	if rf.getLogTermWithIndex(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		xTerm := rf.getLogTermWithIndex(args.PrevLogIndex)
		for index := args.PrevLogIndex; index >= rf.lastSnapshotIndex; index-- {
			if rf.getLogTermWithIndex(index) != xTerm {
				reply.XIndex = index + 1
				DPrintf("[AppendEntries ERROR2]Sever %d ,prevLogIndex %d,Term %d, rf.getLastIndex %d, rf.getLastTerm %d, Confilicing %d",
					rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.getLastIndex(), rf.getLastTerm(),
					reply.XIndex)
				break
			}
		}
		return
	}

	// rule 4
	rf.log = append(rf.log[:args.PrevLogIndex-rf.lastSnapshotIndex+1], args.Entries...)
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.updateCommitInd(StateFollower, args.LeaderCommit)
	}
	DPrintf("[FinishHeartBeat]Server %d, from leader %d(term %d), me.lastIndex %d",
		rf.me, args.LeaderId, args.Term, rf.getLastIndex())

}
