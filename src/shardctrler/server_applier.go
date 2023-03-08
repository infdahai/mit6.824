package shardctrler

import "fmt"

// applier just does send command messages(without processing snapshots).
func (sc *ShardCtrler) applier() {
	for msg := range sc.applyCh {
		if msg.CommandValid {
			sc.mu.Lock()
			if msg.CommandIndex <= sc.lastApplied {
				DPrintf(sc.rf.Me(), "[Applier--Outdate]server[%d] msgCmdInd[%d] kvLaApply[%d]",
					sc.rf.Me()%1000, msg.CommandIndex, sc.lastApplied)
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
				DPrintf(sc.rf.Me(), "[Applier--duplicate]server[%d] clientId[%d] cmdId[%d] LaReply[Err[%s]]",
					sc.rf.Me(), command.ClientId, command.CommandId, reply.Err)

				reply = sc.lastOperations[command.ClientId].LastReply
			} else {
				DPrintf(sc.rf.Me(), "[Applier--ok]server[%d] applyMsg[ CmdInd[%d]]",
					sc.rf.Me()%1000, msg.CommandIndex)

				reply = sc.applyLogToStateMachine(&command)
				if isCheckDuplicate {
					sc.lastOperations[command.ClientId] = LastOpStruct{reply, command.CommandId}
				}
			}

			if _, isLeader := sc.rf.GetState(); !isLeader {
				DPrintf(sc.rf.Me(), "[Applier--!!!isLeader]server[%d] applyMsg[CmdVld[%t] CmdInd[%d]]",
					sc.rf.Me()%1000, msg.CommandValid, msg.CommandIndex)
				sc.mu.Unlock()
				continue
			}

			ch := sc.waitApplyCh[msg.CommandIndex]
			go func() {
				DPrintf(sc.rf.Me(), "[Applier--!!!isLeader]server[%d] applyMsg[CmdVld[%t] CmdInd[%d]]",
					sc.rf.Me()%1000, msg.CommandValid, msg.CommandIndex)
				ch <- &reply
			}()

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
		DPrintf(sc.rf.Me(), "[ApplyStateMachine Join--Start]server[%d] clientId[%d] commandId[%d] err[%s]",
			sc.rf.Me()%1000, command.ClientId%1000, command.CommandId, reply.Err)
		reply.Err = sc.configMachine.Join(command.Servers)
		DPrintf(sc.rf.Me(), "[ApplyStateMachine Join--End]server[%d] clientId[%d] commandId[%d] err[%s]",
			sc.rf.Me()%1000, command.ClientId%1000, command.CommandId, reply.Err)
	case LeaveOp:
		DPrintf(sc.rf.Me(), "[ApplyStateMachine Leave--Start]server[%d] clientId[%d] commandId[%d] err[%s]",
			sc.rf.Me()%1000, command.ClientId%1000, command.CommandId, reply.Err)
		reply.Err = sc.configMachine.Leave(command.GIDs)
		DPrintf(sc.rf.Me(), "[ApplyStateMachine Leave--End]server[%d] clientId[%d] commandId[%d] err[%s]",
			sc.rf.Me()%1000, command.ClientId%1000, command.CommandId, reply.Err)
	case MoveOp:
		DPrintf(sc.rf.Me(), "[ApplyStateMachine Move--Start]server[%d] clientId[%d] commandId[%d] err[%s]",
			sc.rf.Me()%1000, command.ClientId%1000, command.CommandId, reply.Err)
		reply.Err = sc.configMachine.Move(command.Shard, command.GID)
		DPrintf(sc.rf.Me(), "[ApplyStateMachine Move--End]server[%d] clientId[%d] commandId[%d] err[%s]",
			sc.rf.Me()%1000, command.ClientId%1000, command.CommandId, reply.Err)
	case QueryOp:
		DPrintf(sc.rf.Me(), "[ApplyStateMachine Query--Start]server[%d] clientId[%d] commandId[%d] err[%s]",
			sc.rf.Me()%1000, command.ClientId%1000, command.CommandId, reply.Err)
		reply.Config, reply.Err = sc.configMachine.Query(command.Num)
		DPrintf(sc.rf.Me(), "[ApplyStateMachine Query--End]server[%d] clientId[%d] commandId[%d] err[%s] config[%v]",
			sc.rf.Me()%1000, command.ClientId%1000, command.CommandId, reply.Err, reply.Config)
	default:
		panic("unsupported op")
	}
	return reply
}
