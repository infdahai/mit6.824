package shardkv

import (
	"sync"
	"time"

	"6.824/shardctrler"
)

func NewOperationCommand(args *CommandArgs) Command {
	return Command{Operation, *args}
}

func NewConfigurationCommand(config *shardctrler.Config) Command {
	return Command{Configuration, *config}
}

func NewInsertShardsCommand(args *ShardOpReply) Command {
	return Command{InsertShards, *args}
}

func NewDeleteShardsCommand(args *ShardOpArgs) Command {
	return Command{DeleteShards, *args}
}

func NewEmptyEntryCommand() Command {
	return Command{EmptyEntry, nil}
}

func (kv *ShardKV) configureActor() {
	canPerformNextConfig := true
	kv.mu.RLock()
	for _, shard := range kv.stateMachine {
		if shard.Status != Serving {
			canPerformNextConfig = false
			break
		}
	}
	currentConfigNum := kv.currentConfig.Num
	kv.mu.RUnlock()
	if canPerformNextConfig {
		nextConfig := kv.sc.Query(currentConfigNum + 1)
		if nextConfig.Num == currentConfigNum+1 {
			kv.Execute(NewConfigurationCommand(&nextConfig), &CommandReply{})
		}
	}
}

func (kv *ShardKV) migrationActor() {
	kv.mu.RLock()
	gid2shardIDs := kv.getShardIDsByStatus(Pulling)
	var wg sync.WaitGroup
	for gid, shardIds := range gid2shardIDs {

		wg.Add(1)
		go func(servers []string, configNum int, shardIDs []int) {
			defer wg.Done()
			pullTaskArgs := ShardOpArgs{configNum, shardIDs}
			for _, server := range servers {
				var pullTaskReply ShardOpReply
				srv := kv.make_end(server)
				if srv.Call("ShardKV.GetShardsData", &pullTaskArgs, &pullTaskReply) && pullTaskReply.Err == OK {

					kv.Execute(NewInsertShardsCommand(&pullTaskReply), &CommandReply{})
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIds)
	}
	kv.mu.RUnlock()
	wg.Wait()
}

func (kv *ShardKV) gcActor() {
	kv.mu.RLock()
	gid2shardIDs := kv.getShardIDsByStatus(GCing)
	var wg sync.WaitGroup
	for gid, shardIDs := range gid2shardIDs {
		wg.Add(1)
		go func(servers []string, configNum int, shardIDs []int) {
			defer wg.Done()
			gcTaskArgs := ShardOpArgs{configNum, shardIDs}
			for _, server := range servers {
				var gcTaskReply ShardOpReply
				srv := kv.make_end(server)
				if srv.Call("ShardKV.DeleteShardsData", &gcTaskArgs, &gcTaskReply) && gcTaskReply.Err == OK {
					kv.Execute(NewDeleteShardsCommand(&gcTaskArgs), &CommandReply{})
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIDs)
	}
	kv.mu.RUnlock()
	wg.Wait()
}

func (kv *ShardKV) CheckEntryInCurrentTermActor() {
	if !kv.rf.HasLogInCurrentTerm() {
		kv.Execute(NewEmptyEntryCommand(), &CommandReply{})
	}
}

func (kv *ShardKV) Execute(args Command, reply *CommandReply) {
	ind, _, isLeader := kv.rf.Start(args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.UseOrCreateWaitChan(ind)
	kv.mu.Unlock()

	select {
	case res := <-ch:
		reply.Value, reply.Err = res.Value, res.Err
	case <-time.After(TimeoutInterval):
		reply.Err = ErrTimeout
	}

	go kv.RemoveWaitChan(ind)
}
