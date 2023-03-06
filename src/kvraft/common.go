package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type Args struct {
	ClientId  int64
	CommandId int
}

type OpInt int

const (
	PutOp OpInt = iota
	AppendOp
	GetOp
)

type CommandArgs struct {
	Key   string
	Value string
	Op    OpInt // "Put" or "Append" or "Get"
	Args
}

type CommandReply struct {
	Err   Err
	Value string
}

type CommandChanStruct struct {
	ChanReply chan CommandReply
	Outdated  time.Time
}

type LastOpStruct struct {
	LastReply CommandReply
	CommandId int
}

func ConvertOp(opstr string) OpInt {
	switch opstr {
	case "PUT":
		return PutOp
	case "APPEND":
		return AppendOp
	case "GET":
		return GetOp
	default:
		panic("unsupported op")
	}
}
