package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type Args struct {
	ClientId  int64
	RequestId int
}

type PutOrAppend int

const (
	PutOp PutOrAppend = iota
	AppendOp
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    PutOrAppend // "Put" or "Append"

	Args
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	Args
}

type GetReply struct {
	Err   Err
	Value string
}
