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
			DPrintf(kv.rf.Me(), "[configureActor--not finish(can't next config)]server[%d] gid[%d] shard_status[%v] cur_config[%v]",
				kv.rf.Me()%1000, kv.gid, kv.getShardStatus(), kv.currentConfig)
			break
		}
	}
	currentConfigNum := kv.currentConfig.Num
	kv.mu.RUnlock()
	if canPerformNextConfig {
		nextConfig := kv.sc.Query(currentConfigNum + 1)
		DPrintf(kv.rf.Me(), "[configureActor--Query(curConfigNum[%d] nextConfigNum[%d])]server[%d] gid[%d] shard_status[%v] cur_config[%v]",
			currentConfigNum, nextConfig.Num, kv.rf.Me()%1000,
			kv.gid, kv.getShardStatus(), kv.currentConfig)
		if nextConfig.Num == currentConfigNum+1 {

			DPrintf(kv.rf.Me(), "[configureActor--OK(curConfigNum[%d] nextConfigNum[%d])]server[%d] gid[%d] shard_status[%v] cur_config[%v]",
				currentConfigNum, nextConfig.Num, kv.rf.Me()%1000,
				kv.gid, kv.getShardStatus(), kv.currentConfig)
			kv.Execute(NewConfigurationCommand(&nextConfig), &CommandReply{})
		}
	}
}

func (kv *ShardKV) migrationActor() {
	kv.mu.RLock()
	gid2shardIDs := kv.getShardIDsByStatus(Pulling)
	var wg sync.WaitGroup
	for gid, shardIds := range gid2shardIDs {
		DPrintf(kv.rf.Me(), "[migrationActor--routine]server[%d] gid[%d] shardIDs[%v] cur_config[%v] gid2shardIDs[%v]",
			kv.rf.Me()%1000, kv.gid, shardIds, kv.currentConfig, gid2shardIDs)

		wg.Add(1)
		go func(servers []string, configNum int, shardIDs []int) {
			defer wg.Done()
			pullTaskArgs := ShardOpArgs{configNum, shardIDs}
			for _, server := range servers {
				var pullTaskReply ShardOpReply
				srv := kv.make_end(server)
				if srv.Call("ShardKV.GetShardsData", &pullTaskArgs, &pullTaskReply) && pullTaskReply.Err == OK {
					DPrintf(kv.rf.Me(), "[migrationActor--OK]server[%d] gid[%d] pullTaskReply[%v] cur_config[%v]",
						kv.rf.Me()%1000, kv.gid, pullTaskReply, kv.currentConfig)

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
		DPrintf(kv.rf.Me(), "[Server Exec--WrongLeader]server[%d] cmdType[%s]",
			kv.rf.Me()%1000, Opmap2[args.Op])
		return
	}

	ch := make(chan *CommandReply, 1)
	kv.mu.Lock()
	kv.waitApplyCh[ind] = ch
	kv.mu.Unlock()

	select {
	case res := <-ch:
		reply.Value, reply.Err = res.Value, res.Err
	case <-time.After(TimeoutInterval):
		reply.Err = ErrTimeout
	}
	DPrintf(kv.rf.Me(), "[Server Exec--finish]server[%d] reply[%v]",
		kv.rf.Me()%1000, reply)

	go kv.RemoveWaitChan(ind)
}
