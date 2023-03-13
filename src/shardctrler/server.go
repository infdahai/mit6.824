package shardctrler

import (
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	Debug           = false
	TimeoutInterval = 500 * time.Millisecond
)

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configMachine *MemoryConfigStateMachine
	lastApplied   int                        // record the lastApplied to prevent stateMachine from rollback
	waitApplyCh   map[int]chan *CommandReply // index(raft) -> chan

	lastOperations map[int64]LastOpStruct
}

func (sc *ShardCtrler) RemoveWaitChan(ind int) {
	sc.mu.Lock()
	delete(sc.waitApplyCh, ind)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) isRequestDuplicate(clientId int64, commandId int64) bool {
	lastOp, ok := sc.lastOperations[clientId]
	if !ok {
		return false
	}
	return lastOp.CommandId >= commandId
}

func (sc *ShardCtrler) Command(args *CommandArgs, reply *CommandReply) {
	sc.mu.RLock()
	isCheckDuplicate := (args.Op != QueryOp) ||
		(args.Op == QueryOp && args.Num != -1)
	if isCheckDuplicate && sc.isRequestDuplicate(args.ClientId, args.CommandId) {
		lastReply := sc.lastOperations[args.ClientId].LastReply
		reply.Config, reply.Err = lastReply.Config, lastReply.Err
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()

	ind, _, isLeader := sc.rf.Start(*args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := make(chan *CommandReply, 1)

	sc.mu.Lock()
	sc.waitApplyCh[ind] = ch
	sc.mu.Unlock()

	select {
	case result := <-ch:
		reply.Config, reply.Err = result.Config, result.Err
	case <-time.After(TimeoutInterval):
		reply.Err = ErrTimeout
	}
	go sc.RemoveWaitChan(ind)
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(CommandArgs{})
	sc := new(ShardCtrler)
	sc.me = me

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.configMachine = NewMemoryConfigSM()
	sc.waitApplyCh = make(map[int]chan *CommandReply)
	sc.lastOperations = make(map[int64]LastOpStruct)
	go sc.applier()
	return sc
}
