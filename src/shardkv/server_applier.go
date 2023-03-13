package shardkv

import (
	"fmt"

	"6.824/raft"
	"6.824/shardctrler"
)

// a dedicated applier goroutine to apply committed entries to stateMachine, take snapshot and apply snapshot from raft
func (kv *ShardKV) applier() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}
		DPrintf(kv.rf.Me(), "[Applier--start]server[%d]", kv.rf.Me()%1000)
		if msg.CommandValid {
			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastApplied {
				DPrintf(kv.rf.Me(), "[Applier--Outdate]server[%d] msgCmdInd[%d] kvLaApply[%d]",
					kv.rf.Me()%1000, msg.CommandIndex, kv.lastApplied)
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = msg.CommandIndex

			var reply *CommandReply
			command, ok := msg.Command.(Command)
			if !ok {
				panic("applier command is not a Command")
			}

			switch command.Op {
			case Operation:
				command := command.Data.(CommandArgs)
				reply = kv.applyOperation(&msg, &command)
			case Configuration:
				nextConfig := command.Data.(shardctrler.Config)
				reply = kv.applyConfiguration(&nextConfig)
			case InsertShards:
				shardsInfo := command.Data.(ShardOpReply)
				reply = kv.applyInsertShards(&shardsInfo)
			case DeleteShards:
				shardsInfo := command.Data.(ShardOpArgs)
				reply = kv.applyDeleteShards(&shardsInfo)
			case EmptyEntry:
				reply = kv.applyEmptyEntry()
			}

			if _, isLeader := kv.rf.GetState(); isLeader {
				ch := kv.waitApplyCh[msg.CommandIndex]
				go func() { ch <- reply }()
			}

			needSnapshot := kv.needSnapshot(9)
			if needSnapshot {
				kv.takeSnapshot(msg.CommandIndex)
			}

			DPrintf(kv.rf.Me(), "[Applier--CmdFinish]server[%d] msgCmdInd[%d] kvLaApply[%d] reply[%v]",
				kv.rf.Me()%1000, msg.CommandIndex, kv.lastApplied, reply)
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.ReadSnapshot(msg.Snapshot)
				kv.lastApplied = msg.SnapshotIndex
			}
			kv.mu.Unlock()
		} else {
			panic(fmt.Sprintf("unexpected Message %v", msg))
		}
	}
}

func (kv *ShardKV) applyOperation(msg *raft.ApplyMsg, command *CommandArgs) *CommandReply {
	var reply *CommandReply
	shardId := key2shard(command.Key)
	if kv.canServe(shardId) {
		if command.Op != GetOp && kv.isRequestDuplicate(command.ClientId, command.CommandId) {
			DPrintf(kv.rf.Me(), "[Applier--duplicate]server[%d] clientId[%d] cmdId[%d] LaReply[Err[%s]]",
				kv.rf.Me(), command.ClientId, command.CommandId, reply.Err)
			*reply = kv.lastOperations[command.ClientId].LastReply
			return reply
		} else {
			reply = kv.applyLogToStateMachine(command, shardId)
			if command.Op != GetOp {
				kv.lastOperations[command.ClientId] = LastOpStruct{LastReply: *reply, CommandId: command.CommandId}
			}
			return reply
		}
	}
	return &CommandReply{ErrWrongGroup, ""}
}

func (kv *ShardKV) applyConfiguration(nextConfig *shardctrler.Config) *CommandReply {
	if nextConfig.Num == kv.currentConfig.Num+1 {
		kv.updateShardStatus(nextConfig)
		kv.lastConfig = kv.currentConfig
		kv.currentConfig = *nextConfig
		return &CommandReply{OK, ""}
	}
	return &CommandReply{ErrOutdated, ""}
}

func (kv *ShardKV) applyInsertShards(shardInfo *ShardOpReply) *CommandReply {
	if shardInfo.ConfigNum == kv.currentConfig.Num {
		for shardID, shardData := range shardInfo.Shards {
			shard := kv.stateMachine[shardID]
			if shard.Status == Pulling {
				for key, val := range shardData {
					shard.KV[key] = val
				}
				shard.Status = GCing
			} else {
				break
			}
		}
		for clientID, operation := range shardInfo.LastOperations {
			if lastoperations, ok := kv.lastOperations[clientID]; !ok ||
				lastoperations.CommandId < operation.CommandId {
				kv.lastOperations[clientID] = operation
			}
		}
		return &CommandReply{OK, ""}
	}
	return &CommandReply{ErrOutdated, ""}
}

func (kv *ShardKV) applyDeleteShards(shardInfo *ShardOpArgs) *CommandReply {
	if shardInfo.ConfigNum == kv.currentConfig.Num {
		for _, shardID := range shardInfo.ShardIDs {
			shard := kv.stateMachine[shardID]
			if shard.Status == GCing {
				// for me, GC->Serving
				shard.Status = Serving
			} else if shard.Status == BePulling {
				// for remote, BePulling->Serving
				kv.stateMachine[shardID] = NewShard()
			} else {
				break
			}
		}
		return &CommandReply{OK, ""}
	}
	return &CommandReply{OK, ""}
}

func (kv *ShardKV) applyEmptyEntry() *CommandReply {
	return &CommandReply{OK, ""}
}

func (kv *ShardKV) applyLogToStateMachine(command *CommandArgs, shardId int) *CommandReply {
	var reply *CommandReply
	switch command.Op {
	case GetOp:
		reply.Value, reply.Err = kv.stateMachine[shardId].Get(command.Key)
	case PutOp:
		reply.Err = kv.stateMachine[shardId].Put(command.Key, command.Value)
	case AppendOp:
		reply.Err = kv.stateMachine[shardId].Append(command.Key, command.Value)
	}
	return reply
}
