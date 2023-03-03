package raft

import "time"

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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		now := time.Now()
		election_timeout := GetRand(int64(rf.me))
		time.Sleep(time.Duration(election_timeout) * time.Millisecond)
		rf.mu.Lock()
		//  not leader.
		// candidate and follower start election when election timeout elapsed.
		if rf.lastResetElectionTime.Before(now) && rf.state != StateLeader {
			rf.ChangeState(StateCandidate, true)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) genRequestVoteRequest() *RequestVoteArgs {
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
	}
	return args
}

//lock_guard
func (rf *Raft) CandidateElection() {
	reqArgs := rf.genRequestVoteRequest()

	rf.DPrintf(dinfo, delection, "[server:%d][request:%+v]electime elpased out => Candidate",
		rf.me, reqArgs)

	grantedVotes := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			var reply RequestVoteReply
			if rf.sendRequestVote(peer, reqArgs, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// maybe rf == follower or votes outdated.
				if rf.state != StateCandidate || reqArgs.Term < rf.currentTerm {
					return
				}

				if reply.Term > reqArgs.Term {
					rf.DPrintf(dinfo, delection, "[server:%d][reply:%+v]electime => Follower,currentT:%d",
						rf.me, reply, rf.currentTerm)
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
					}
					rf.ChangeState(StateFollower, false)
					return
				}

				// state == candidate
				if rf.currentTerm == reqArgs.Term && reply.VoteGranted {
					grantedVotes += 1
					if rf.IsMajority(grantedVotes) {
						rf.DPrintf(dinfo, delection, "[server:%d][reply:%+v]electime => Leader",
							rf.me, reply)
						rf.ChangeState(StateLeader, true)
						return
					}
				}
			}
		}(peer)

	}

}

//TODO(infdahai): remember lastHeartBeat is setting on grant successful votes and reply for appendEntry.
//
// Invoked by candidates to gather votes.
//
// then args are processed by candidate.
// reply is processed by server.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer rf.DPrintf(ddebug, dvote, "[server %d]RequestVote[voted=%d][args: %+v]", rf.me, rf.votedFor, args)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// rule 1
	if args.Term < rf.currentTerm {
		DPrintf("[ElectionReject++++++]Server %d reject %d, MYterm %d, candidate term %d",
			rf.me, args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	// update current term. But not update reply.Term
	if args.Term > rf.currentTerm {
		DPrintf("[ElectionToFollower++++++]Server %d(term %d) into follower,candidate %d(term %d) ",
			rf.me, rf.currentTerm, args.CandidateId, args.Term)
		rf.currentTerm = args.Term
		rf.ChangeState(StateFollower, false)

		reply.Term = args.Term
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isLogUpToDate(args) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastResetElectionTime = time.Now()
		rf.persist()
		DPrintf("[ElectionSUCCESS+++++++]Server %d voted for %d!", rf.me, args.CandidateId)
		return
	}

	DPrintf("[ElectionReject+++++++]Server %d reject %d, Have voter for %d", rf.me, args.CandidateId, rf.votedFor)

}
