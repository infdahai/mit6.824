package raft

import (
	"fmt"
	"log"
	"strings"
	"time"
)

// Debugging
const (
	Debug     = true
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
	prefix := fmt.Sprintf("%sTime:%s %s:%-5s [State:%s Term:%02d Id:%d] %s", preamble, Microseconds(time.Now()),
		string(topic1), string(topic2), rf.String(), rf.currentTerm, rf.me, epilogue)
	format = prefix + format
	return fmt.Sprintf(format, a...)
}

func Microseconds(t time.Time) string {
	return fmt.Sprintf("%06d", t.Sub(debugStart).Microseconds()/100)
}

func (rf *Raft) FormatLog() string {
	s := ""
	for _, i := range rf.log.Entries {
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
