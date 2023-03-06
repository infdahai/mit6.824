package kvraft

import "fmt"

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}
		DPrintf(kv.rf.Me(), "[ApplierStart--]server[%d] applyMsg[%v]", kv.rf.Me()%1000, msg)

		if msg.CommandValid {
			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastApplied {
				// DPrintf(kv.rf.Me(), "{Node %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been restored",
				// 	kv.rf.Me(), msg, kv.lastApplied)
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
				// DPrintf(kv.rf.Me(), "{Node %v} doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v", kv.rf.Me(),
				// 	msg, kv.lastOperations[command.ClientId], command.ClientId)
				reply = kv.lastOperations[command.ClientId].LastReply
			} else {
				reply = kv.applyLogToStateMachine(&command)
				if command.Op != GetOp {
					kv.lastOperations[command.ClientId] = LastOpStruct{reply, command.CommandId}
				}
			}

			if _, isLeader := kv.rf.GetState(); !isLeader {
				continue
			}

			ch := kv.UseOrCreateWaitChan(msg.CommandIndex)
			ch <- &reply

			needSnapshot := kv.needSnapshot(9)
			if needSnapshot {
				kv.takeSnapshot(msg.CommandIndex)
			}
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
