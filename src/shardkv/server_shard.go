package shardkv

import (
	"6.824/shardctrler"
)

type ShardStatus uint8

const (
	Serving   ShardStatus = iota
	Pulling               // pull shards from others
	BePulling             // shards be pulled
	GCing
)

type Shard struct {
	KV     map[string]string
	Status ShardStatus
}

func NewShard() *Shard {
	return &Shard{make(map[string]string), Serving}
}

func (shard *Shard) Get(key string) (string, Err) {
	if val, ok := shard.KV[key]; ok {
		return val, OK
	}
	return "", ErrNoKey
}

func (shard *Shard) Put(key, val string) Err {
	shard.KV[key] = val
	return OK
}

func (shard *Shard) Append(key, val string) Err {
	shard.KV[key] += val
	return OK
}

func (shard *Shard) deepCopy() map[string]string {
	newShard := make(map[string]string)
	for k, v := range shard.KV {
		newShard[k] = v
	}
	return newShard
}

func (shard *Shard) Change(kvmap map[string]string) {
	shard.KV = kvmap
}

func (kv *ShardKV) updateShardStatus(nextConfig *shardctrler.Config) {
	for shardId, newgid := range nextConfig.Shards {
		oldgid := kv.currentConfig.Shards[shardId]
		if oldgid == kv.gid && kv.gid != newgid {
			kv.stateMachine[shardId].Status = BePulling
		} else if oldgid != kv.gid && newgid == kv.gid {
			kv.stateMachine[shardId] = &Shard{KV: make(map[string]string), Status: Pulling}
		}
	}
}

func (kv *ShardKV) getShardIDsByStatus(status ShardStatus) map[int][]int {
	gid2shardIDs := make(map[int][]int)
	for shardID, shard := range kv.stateMachine {
		if shard.Status == status {
			oldgid := kv.lastConfig.Shards[shardID]
			gid2shardIDs[oldgid] = append(gid2shardIDs[oldgid], shardID)
		}
	}
	return gid2shardIDs
}

func (kv *ShardKV) GetShardsData(args *ShardOpArgs, reply *ShardOpReply) {
	// only pull shards from leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	if kv.currentConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	reply.Shards = make(map[int]map[string]string)
	for _, shardID := range args.ShardIDs {
		reply.Shards[shardID] = kv.stateMachine[shardID].deepCopy()
	}

	reply.LastOperations = make(map[int64]LastOpStruct)
	for clienID, op := range kv.lastOperations {
		reply.LastOperations[clienID] = op.deepCopy()
	}
	reply.ConfigNum, reply.Err = args.ConfigNum, OK
}

func (kv *ShardKV) DeleteShardsData(args *ShardOpArgs, reply *ShardOpReply) {
	// only delete shards when role is leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	if kv.currentConfig.Num > args.ConfigNum {
		reply.Err = OK
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	var commandReply CommandReply
	kv.Execute(NewDeleteShardsCommand(args), &commandReply)
	reply.Err = commandReply.Err
}
