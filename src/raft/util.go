package raft

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"
)

// Debugging
const (
	Debug     = false
	PrintLock = false
)

type logStr string

const (
	dlog      logStr = "LOG"  // AppendeEntries
	dvote     logStr = "VOTE" // ReqeustVotes
	delection logStr = "ELECT"
	dheart    logStr = "HEART"
	dcommit   logStr = "COMMIT"
	dclient   logStr = "CLIENT"
	dtrace    logStr = "TRACE"
	dlock     logStr = "LOCK"
	dlock2    logStr = "GLOCK"

	ddebug logStr = "DEBUG"
	dwarn  logStr = "WARN"
	dinfo  logStr = "INFO"
	derror logStr = "ERROR"
)

var debugStart time.Time

func init() {
	debugStart = time.Now()
}

type Color string

const (
	Green  Color = "\033[1;32;40m"
	Yellow Color = "\033[1;33;40m"
	Blue   Color = "\033[1;34;40m"
	Red    Color = "\033[1;31;40m"
	White  Color = "\033[1;37;40m"

	Def Color = "\033[0m\n"
)

func ColorStr(topic logStr) string {
	var col Color
	switch topic {
	case dwarn:
		col = Yellow
	case dinfo:
		col = Green
	case derror:
		col = Red
	case ddebug:
		col = Blue

	case dlock2:
		col = White
	default:
		col = Green
	}
	res := string(col) + "%s" + string(Def)
	return res
}

/* ================ log print ================
 *================================================================
 */

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) DPrintf(topic1 logStr, topic2 logStr, format string, a ...interface{}) {
	if Debug {
		colfmt := ColorStr(topic1)
		s := rf.Dlog(topic1, topic2, format, a...)
		if s != "" {
			log.Printf(colfmt, s)
		} else {
			return
		}
	}
}

const Padding = "		"

/*
dlock code template :

	rf.DPrintf(dwarn, dlock, "Sync: acq mu.lock")
	rf.mu.Lock()
	rf.DPrintf(dlock2, dlock, "Sync: get mu.lock")

	defer rf.DPrintf(ddebug, dlock, "Sync: release mu.lock")
	defer rf.mu.Unlock()
*/

// topic1 : info/error
// topic2 : such as log,vote, etc.
func (rf *Raft) Dlog(topic1 logStr, topic2 logStr, format string, a ...interface{}) string {
	if topic2 == dlock && !PrintLock {
		return ""
	}
	preamble := strings.Repeat(Padding, rf.me)
	epilogue := strings.Repeat(Padding, len(rf.peers)-rf.me-1)
	prefix := fmt.Sprintf("%sTime:%s %s:%-5s [State:%s Term:%02d Id:%d lastSSInd:%d lastSSTerm:%d] %s",
		preamble, Microseconds(time.Now()),
		string(topic1), string(topic2), rf.String(),
		rf.currentTerm, rf.me, rf.lastSnapshotIndex,
		rf.lastSnapshotTerm, epilogue)
	format = prefix + format
	return fmt.Sprintf(format, a...)
}

func Microseconds(t time.Time) string {
	return fmt.Sprintf("%06d", t.Sub(debugStart).Microseconds()/100)
}

func (rf *Raft) FormatLog() string {
	s := ""
	for _, i := range rf.log {
		s += fmt.Sprintf("%v ", i)
	}
	return s
}

func (rf *Raft) FormatStateOnly() string {
	return fmt.Sprintf("commitInd=%d, lastApplie=%d, nextInd=%v, matchInd=%v",
		rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex)
}

func (rf *Raft) FormatState() string {
	return fmt.Sprintf("%s --> cur log: %v", rf.FormatStateOnly(), rf.FormatLog())
}

func GetRand(peer int64) int {
	rand.Seed(time.Now().Unix() + peer)
	return rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin
}

/*
* importantly:
*	 reset election timeing:
*		 1. receive append entries from leader.
*	   2. become a candidate.
*		 3. grant votes for other peers.
 */
// reset election timeout
// lock_guard
func (rf *Raft) ChangeState(ns StateType, resetTime bool) {
	rf.state = ns

	if ns == StateFollower {
		rf.votedFor = -1
		rf.persist()
		if resetTime {
			rf.lastResetElectionTime = time.Now()
		}
		return
	}

	if ns == StateCandidate {
		rf.votedFor = rf.me
		rf.currentTerm += 1
		rf.persist()
		rf.CandidateElection()
		rf.lastResetElectionTime = time.Now()
		return
	}

	if ns == StateLeader {
		rf.votedFor = -1
		rf.persist()

		// update nextIndex && matchIndex
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.peers {
			rf.nextIndex[i] = rf.getLastIndex() + 1
			rf.matchIndex[i] = 0
		}
		rf.matchIndex[rf.me] = rf.getLastIndex()
		rf.lastResetElectionTime = time.Now()
		return
	}

}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) IsMajority(num int) bool {
	return (num >= (len(rf.peers)/2 + 1))
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

func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}
