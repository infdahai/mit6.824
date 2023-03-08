package shardctrler

import "fmt"

// applier just does send command messages(without processing snapshots).
func (sc *ShardCtrler) applier() {
	for msg := range sc.applyCh {
		if msg.CommandValid {
			sc.mu.Lock()
			if msg.CommandIndex <= sc.lastApplied {
				sc.mu.Unlock()
				continue
			}
			sc.lastApplied = msg.CommandIndex

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
				go func() {
					ch <- &reply
				}()
			}

			sc.mu.Unlock()
		} else {
			panic(fmt.Sprintf("unexpected Message %v", msg))
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
	default:
		panic("unsupported op")
	}
	return reply
}
