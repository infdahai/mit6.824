package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrOutdated    = "ErrOutdated"
	ErrNotReady    = "ErrNotReady"
)

type Err string

type Args struct {
	ClientId  int64
	CommandId int64
}

type OpInt int

const (
	PutOp OpInt = iota
	AppendOp
	GetOp
)

// type CommandArgs struct {
// 	Servers map[int][]string // Join, new GID -> servers mappings
// 	GIDs    []int            // Leave
// 	Shard   int              // Move
// 	GID     int              // Move
// 	Num     int              // Query, desired config number
// 	Op      OpInt
// 	Args
// }

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

type ShardOpArgs struct {
	ConfigNum int
	ShardIDs  []int
}

type ShardOpReply struct {
	Err            Err
	Shards         map[int]map[string]string
	LastOperations map[int64]LastOpStruct
	ConfigNum      int
}

type LastOpStruct struct {
	LastReply *CommandReply
	CommandId int64
}

func (op *LastOpStruct) deepCopy() LastOpStruct {
	lastop := LastOpStruct{CommandId: op.CommandId}
	lastop.LastReply = &CommandReply{Err: op.LastReply.Err, Value: op.LastReply.Value}
	return lastop
}

type CommandType uint8

const (
	Operation CommandType = iota
	Configuration
	InsertShards
	DeleteShards
	EmptyEntry
)

type Command struct {
	Op   CommandType
	Data interface{}
}
