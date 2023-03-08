package shardctrler

import (
	"fmt"
	"log"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

// one group[gid] can maintain multiple shards(len(shards)>=1)
// valid gid is non-zero

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type OpInt int

const (
	JoinOp OpInt = iota
	LeaveOp
	MoveOp
	QueryOp
)

type Args struct {
	ClientId  int64
	CommandId int64
}

type CommandArgs struct {
	Servers map[int][]string // Join, new GID -> servers mappings
	GIDs    []int            // Leave
	Shard   int              // Move
	GID     int              // Move
	Num     int              // Query, desired config number
	Op      OpInt
	Args
}

type CommandReply struct {
	Err    Err
	Config Config
}

type LastOpStruct struct {
	LastReply CommandReply
	CommandId int64
}

func ConvertOp(opstr string) OpInt {
	switch opstr {
	case "JOIN":
		return JoinOp
	case "LEAVE":
		return LeaveOp
	case "MOVE":
		return MoveOp
	case "QUERY":
		return QueryOp
	default:
		panic("unsupported op")
	}
}

var Opmap = [...]string{
	"Join",
	"Leave",
	"Move",
	"Query",
}

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
