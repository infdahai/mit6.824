package kvraft

import "fmt"

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}
		DPrintf(kv.rf.Me(), "[Applier--Start]server[%d] applyMsg[CmdVld[%t] CmdInd[%d] SsVld[%t] SsTerm[%d] SsInd[%d]]",
			kv.rf.Me()%1000,
			msg.CommandValid, msg.CommandIndex, msg.SnapshotValid,
			msg.SnapshotTerm, msg.SnapshotIndex)

		if msg.CommandValid {
			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastApplied {
				DPrintf(kv.rf.Me(), "[Applier--Outdate]server[%d] msgCmdInd[%d] kvLaApply[%d]",
					kv.rf.Me()%1000, msg.CommandIndex, kv.lastApplied)
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = msg.CommandIndex

			var reply CommandReply
			command, ok := msg.Command.(CommandArgs)
			if !ok {
				panic("applier command is not a CommandArgs")
			}

			if command.Op != GetOp && kv.isRequestDuplicate(command.ClientId, command.CommandId) {
				reply = kv.lastOperations[command.ClientId].LastReply
				DPrintf(kv.rf.Me(), "[Applier--duplicate]server[%d] clientId[%d] cmdId[%d] LaReply[Err[%s]]",
					kv.rf.Me(), command.ClientId, command.CommandId, reply.Err)
			} else {
				DPrintf(kv.rf.Me(), "[Applier--ok]server[%d] applyMsg[CmdVld[%t] CmdInd[%d] SsVld[%t] SsTerm[%d] SsInd[%d]]",
					kv.rf.Me()%1000,
					msg.CommandValid, msg.CommandIndex, msg.SnapshotValid,
					msg.SnapshotTerm, msg.SnapshotIndex)
				reply = kv.applyLogToStateMachine(&command)
				if command.Op != GetOp {
					kv.lastOperations[command.ClientId] = LastOpStruct{LastReply: reply, CommandId: command.CommandId}
				}
			}

			if _, isLeader := kv.rf.GetState(); isLeader {
				DPrintf(kv.rf.Me(), "[Applier--isLeader]server[%d] applyMsg[CmdVld[%t] CmdInd[%d] SsVld[%t] SsTerm[%d] SsInd[%d]]",
					kv.rf.Me()%1000,
					msg.CommandValid, msg.CommandIndex, msg.SnapshotValid,
					msg.SnapshotTerm, msg.SnapshotIndex)
				ch := kv.UseOrCreateWaitChan(msg.CommandIndex)
				ch <- &reply
			}

			needSnapshot := kv.needSnapshot(9)
			if needSnapshot {
				DPrintf(kv.rf.Me(), "[Applier--TakeSnapshot]server[%d] applyMsg[SsTerm[%d] SsInd[%d]]",
					kv.rf.Me()%1000, msg.SnapshotTerm, msg.SnapshotIndex)
				kv.takeSnapshot(msg.CommandIndex)
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				DPrintf(kv.rf.Me(), "[Applier--ReadSnapshot]server[%d] applyMsg[SsTerm[%d] SsInd[%d]]",
					kv.rf.Me()%1000, msg.SnapshotTerm, msg.SnapshotIndex)
				kv.ReadSnapshot(msg.Snapshot)
				kv.lastApplied = msg.SnapshotIndex
			}
			kv.mu.Unlock()
		} else {
			panic(fmt.Sprintf("unexpected Message %v", msg))
		}
	}
}

func (kv *KVServer) applyLogToStateMachine(command *CommandArgs) CommandReply {
	var reply CommandReply
	switch command.Op {
	case GetOp:
		reply.Value, reply.Err = kv.kvMachine.Get(command.Key)
	case PutOp:
		reply.Err = kv.kvMachine.Put(command.Key, command.Value)
	case AppendOp:
		reply.Err = kv.kvMachine.Append(command.Key, command.Value)
	}
	return reply
}
