package kvraft

import "fmt"

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}

		if msg.CommandValid {
			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastApplied {
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
			} else {
				reply = kv.applyLogToStateMachine(&command)
				if command.Op != GetOp {
					kv.lastOperations[command.ClientId] = LastOpStruct{LastReply: reply, CommandId: command.CommandId}
				}
			}

			if _, isLeader := kv.rf.GetState(); isLeader {
				ch := kv.UseOrCreateWaitChan(msg.CommandIndex)
				go func() { ch <- &reply }()
			}

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
