package shardkv

import (
	"fmt"
	"log"
)

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
	ConfigNum      int
	Shards         map[int]map[string]string
	LastOperations map[int64]LastOpStruct
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

func (command Command) String() string {
	return fmt.Sprintf("{Type:%v,Data:%v}", command.Op, command.Data)
}

var Opmap1 = [...]string{
	"Put",
	"Append",
	"Get",
}

var Opmap2 = [...]string{
	"Operation",
	"Configuration",
	"InsertShards",
	"DeleteShards",
	"EmptyEntry",
}

//======================== Log

type Color string

const (
	Green  Color = "\033[1;32;40m"
	Yellow Color = "\033[1;33;40m"
	Blue   Color = "\033[1;34;40m"
	Red    Color = "\033[1;31;40m"
	White  Color = "\033[1;37;40m"

	Def Color = "\033[0m\n"
)

func ColorStr(topic int) string {
	var col Color
	topic = topic % 5
	switch topic {
	case 0:
		col = Green
	case 1:
		col = Yellow
	case 2:
		col = Blue
	case 3:
		col = Red
	case 4:
		col = White
	}
	res := string(col) + "%s" + string(Def)
	return res
}

func DPrintf(id int, format string, a ...interface{}) (n int, err error) {
	if Debug {
		colstr := ColorStr(id)
		str := fmt.Sprintf(format, a...)
		log.Printf(colstr, str)
	}
	return
}
