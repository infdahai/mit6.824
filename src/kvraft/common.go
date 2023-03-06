package kvraft

import (
	"fmt"
	"log"
	"time"
)

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
	ChanReply chan *CommandReply
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

var Opmap = [...]string{
	"Put",
	"Append",
	"Get",
}

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
