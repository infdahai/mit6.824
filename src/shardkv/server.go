package shardkv

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const (
	Debug                = false
	TimeoutInterval      = 500 * time.Millisecond
	ConfigurationTimeout = 90 * time.Millisecond
	MigrationTimeout     = 50 * time.Millisecond
	GCTimeout            = 50 * time.Millisecond
	EmptyEntryTimeout    = 200 * time.Millisecond
)

type ShardKV struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	make_end func(string) *labrpc.ClientEnd
	gid      int
	sc       *shardctrler.Clerk

	maxraftstate int // snapshot if log grows this big
	lastApplied  int // record the lastApplied to prevent stateMachine from rollback

	lastConfig    shardctrler.Config
	currentConfig shardctrler.Config

	stateMachine   map[int]*Shard             // KV stateMachines
	lastOperations map[int64]LastOpStruct     // determine whether log is duplicated by recording the last commandId and response corresponding to the clientId
	waitApplyCh    map[int]chan *CommandReply // notify client goroutine by applier goroutine to response
}

func (kv *ShardKV) UseOrCreateWaitChan(ind int) chan *CommandReply {
	if _, ok := kv.waitApplyCh[ind]; !ok {
		kv.waitApplyCh[ind] = make(chan *CommandReply, 1)
	}
	return kv.waitApplyCh[ind]
}

func (kv *ShardKV) RemoveWaitChan(ind int) {
	kv.mu.Lock()
	delete(kv.waitApplyCh, ind)
	kv.mu.Unlock()
}

// check whether this raft group can serve this shard at present
func (kv *ShardKV) canServe(shardID int) bool {
	return kv.currentConfig.Shards[shardID] == kv.gid &&
		(kv.stateMachine[shardID].Status == Serving || kv.stateMachine[shardID].Status == GCing)
}

func (kv *ShardKV) isRequestDuplicate(clientId int64, commandId int64) bool {
	lastOp, ok := kv.lastOperations[clientId]
	if !ok {
		return false
	}
	return lastOp.CommandId >= commandId
}

func (kv *ShardKV) Command(args *CommandArgs, reply *CommandReply) {
	kv.mu.RLock()
	// return result directly without raft layer's participation if request is duplicated
	if args.Op != GetOp && kv.isRequestDuplicate(args.ClientId, args.CommandId) {
		lastReply := kv.lastOperations[args.ClientId].LastReply
		reply.Value, reply.Err = lastReply.Value, lastReply.Err

		kv.mu.RUnlock()
		return
	}
	// return ErrWrongGroup directly to let client fetch latest configuration and
	// perform a retry if this key can't be served by this shard at present
	if !kv.canServe(key2shard(args.Key)) {
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	kv.Execute(NewOperationCommand(args), reply)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(CommandArgs{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardOpArgs{})
	labgob.Register(ShardOpReply{})

	kv := new(ShardKV)
	kv.dead = 0
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sc = shardctrler.MakeClerk(ctrlers)

	kv.lastConfig = shardctrler.DefaultConfig()
	kv.currentConfig = shardctrler.DefaultConfig()
	kv.lastOperations = make(map[int64]LastOpStruct)
	kv.waitApplyCh = make(map[int]chan *CommandReply)
	kv.stateMachine = make(map[int]*Shard)
	kv.lastApplied = 0

	snapshot := persister.ReadSnapshot()
	kv.ReadSnapshot(snapshot)

	// start applier goroutine to apply committed logs to stateMachine
	go kv.applier()
	// start configuration monitor goroutine to fetch latest configuration
	go kv.Daemon(kv.configureActor, ConfigurationTimeout)
	// start migration monitor goroutine to pull related shards
	go kv.Daemon(kv.migrationActor, MigrationTimeout)
	// start gc monitor goroutine to delete useless shards in remote groups
	go kv.Daemon(kv.gcActor, GCTimeout)
	// start entry-in-currentTerm monitor goroutine to advance commitIndex by appending empty entries in current term periodically to avoid live locks
	go kv.Daemon(kv.CheckEntryInCurrentTermActor, EmptyEntryTimeout)

	return kv
}

func (kv *ShardKV) Daemon(actor func(), timeout time.Duration) {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			actor()
		}
		time.Sleep(timeout)
	}
}
