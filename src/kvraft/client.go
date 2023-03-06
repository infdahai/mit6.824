package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"

	mathrand "math/rand"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	clientId  int64
	commandId int
	leaderId  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func NServerRand(len int) int {
	return mathrand.Intn(len)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.leaderId = NServerRand(len(servers))
	DPrintf(int(ck.clientId), "[InitClerk---clerk]clientId[%d] commandId[%d] leaderId[%d] ",
		ck.clientId%1000, ck.commandId, ck.leaderId)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) args() Args {
	return Args{ClientId: ck.clientId, CommandId: ck.commandId}
}

func (ck *Clerk) nextLeader(leaderId int) int {
	return (leaderId + 1) % len(ck.servers)
}

const RetryInterval = 300 * time.Millisecond

func (ck *Clerk) Command(req *CommandArgs) string {
	req.Args = ck.args()
	server := ck.leaderId
	DPrintf(int(ck.clientId), "[ClientSend Command---req]clientId[%d] commandId[%d] Operation[%s] Key[%s] Val[%s]",
		req.ClientId%1000, req.CommandId, Opmap[req.Op], req.Key, req.Value)

	for {
		var reply CommandReply
		ok := ck.servers[server].Call("KVServer.Command", req, &reply)
		DPrintf(int(ck.clientId), "[ClientRecv Command--reply]clientId[%d] commandId[%d] ok[%t] val[%s] err[%s]",
			req.ClientId%1000, req.CommandId, ok, reply.Value, reply.Err)
		if !ok || reply.Err == ErrTimeout {
			server = ck.nextLeader(server)
			continue
		}
		if reply.Err == ErrWrongLeader {
			server = ck.nextLeader(server)
			time.Sleep(RetryInterval)
		}
		ck.commandId++
		ck.leaderId = server
		return reply.Value
	}
}

func (ck *Clerk) Get(key string) string {
	return ck.Command(&CommandArgs{Key: key, Op: GetOp})
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) Put(key string, value string) {
	ck.Command(&CommandArgs{Key: key, Value: value, Op: PutOp})
}
func (ck *Clerk) Append(key string, value string) {
	ck.Command(&CommandArgs{Key: key, Value: value, Op: AppendOp})
}
