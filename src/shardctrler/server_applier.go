package shardctrler

// applier just does send command messages(without processing snapshots).
func (sc *ShardCtrler) applier() {
	for msg := range sc.applyCh {
		if msg.CommandValid {
			if msg.CommandIndex <= sc.lastApplied {
				continue
			}
			sc.mu.Lock()

			var reply CommandReply
			command, ok := msg.Command.(CommandArgs)
			if !ok {
				panic("applier command is not a CommandArgs")
			}

			isCheckDuplicate := (command.Op != QueryOp) ||
				(command.Op == QueryOp && command.Num != -1)
			if isCheckDuplicate && sc.isRequestDuplicate(command.ClientId, command.CommandId) {
				reply = sc.lastOperations[command.ClientId].LastReply
			} else {
				reply = sc.applyLogToStateMachine(&command)
				if isCheckDuplicate {
					sc.lastOperations[command.ClientId] = LastOpStruct{reply, command.CommandId}
				}
			}

			if _, isLeader := sc.rf.GetState(); isLeader {
				ch := sc.waitApplyCh[msg.CommandIndex]
				ch <- &reply
			}

			sc.lastApplied = msg.CommandIndex
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) applyLogToStateMachine(command *CommandArgs) CommandReply {
	var reply CommandReply
	switch command.Op {
	case JoinOp:
		reply.Err = sc.configMachine.Join(command.Servers)
	case LeaveOp:
		reply.Err = sc.configMachine.Leave(command.GIDs)
	case MoveOp:
		reply.Err = sc.configMachine.Move(command.Shard, command.GID)
	case QueryOp:
		reply.Config, reply.Err = sc.configMachine.Query(command.Num)
	}
	return reply
}
