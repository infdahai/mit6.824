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

func ConvertShardStatus(status ShardStatus) string {
	switch status {
	case Serving:
		return "Serving"
	case Pulling:
		return "Pulling"
	case BePulling:
		return "BePulling"
	case GCing:
		return "GCing"
	default:
		panic("unknown ShardStatus")
	}
}

type Shard struct {
	KV     map[string]string
	Status ShardStatus
}

func (kv *ShardKV) getShardStatus() map[int]string {
	ret := make(map[int]string)
	for shardid, shard := range kv.stateMachine {
		ret[shardid] = ConvertShardStatus(shard.Status)
	}
	return ret
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
	// TODO(infdahai): important!!! remember gid !=0 is valid.
	for i := 0; i < shardctrler.NShards; i++ {
		oldgid := kv.currentConfig.Shards[i]
		newgid := nextConfig.Shards[i]
		if oldgid != kv.gid && newgid == kv.gid {
			if oldgid != 0 {
				kv.stateMachine[i].Status = Pulling
			}
		}
		if oldgid == kv.gid && newgid != kv.gid {
			if newgid != 0 {
				kv.stateMachine[i].Status = BePulling
			}
		}
	}

	DPrintf(kv.rf.Me(), "[Server updateShardStatus--]server[%d]  shardStatus[%v]",
		kv.rf.Me()%1000, kv.getShardStatus())
}

func (kv *ShardKV) getShardIDsByStatus(status ShardStatus) map[int][]int {
	gid2shardIDs := make(map[int][]int)
	for shardID, shard := range kv.stateMachine {
		if shard.Status == status {
			oldgid := kv.lastConfig.Shards[shardID]
			// TODO(infdahai): important!!! remember gid !=0 is valid.
			if oldgid != 0 {
				if _, ok := gid2shardIDs[oldgid]; !ok {
					//BUG: make int slice entry.
					gid2shardIDs[oldgid] = make([]int, 0)
				}
				gid2shardIDs[oldgid] = append(gid2shardIDs[oldgid], shardID)
			}
		}
	}
	return gid2shardIDs
}

func (kv *ShardKV) GetShardsData(args *ShardOpArgs, reply *ShardOpReply) {
	// only pull shards from leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf(kv.rf.Me(), "[Server GetShardsData--Err(WrongLeader)]server[%d] gid[%d] args[%v] reply[%v]",
			kv.rf.Me()%1000, kv.gid, args, reply)
		return
	}

	kv.mu.RLock()
	defer kv.mu.RUnlock()
	DPrintf(kv.rf.Me(), "[Server GetShardsData--]server[%d] gid[%d] args[%v] reply[%v]",
		kv.rf.Me()%1000, kv.gid, args, reply)

	if kv.currentConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		DPrintf(kv.rf.Me(), "[Server GetShardsData--NotReady]server[%d] gid[%d] args[%v] argsnum[%d] curConfig[%v] reply[%v]",
			kv.rf.Me()%1000, kv.gid, args, args.ConfigNum, kv.currentConfig, reply)
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

	DPrintf(kv.rf.Me(), "[Server GetShardsData--OK]server[%d] gid[%d] args[%v] reply[%v]",
		kv.rf.Me()%1000, kv.gid, args, reply)

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
