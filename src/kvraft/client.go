package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"

	"6.824/labrpc"

	mathrand "math/rand"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	clientId  int64
	requestId int
	LeaderId  int
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
	ck.LeaderId = NServerRand(len(servers))
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
	return Args{ClientId: ck.clientId, RequestId: ck.requestId}
}

func (ck *Clerk) nextLeader(leaderId int) int {
	return (leaderId + 1) % len(ck.servers)
}

const RetryInterval = 300 * time.Millisecond

func (ck *Clerk) Get(key string) string {
	ck.requestId++
	server := ck.LeaderId
	getArgs := GetArgs{Key: key, Args: ck.args()}

	for {
		var getReply GetReply
		ok := ck.servers[server].Call("KVServer.Get", &getArgs, &getReply)
		if !ok || getReply.Err == ErrTimeout {
			server = ck.nextLeader(server)
			continue
		}
		if getReply.Err == ErrNoKey {
			return ""
		}
		if ok && getReply.Err == OK {
			ck.LeaderId = server
			return getReply.Value
		}
		// getReply.Err == ErrWrongLeader
		server = ck.nextLeader(server)
		time.Sleep(RetryInterval)
	}
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
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.requestId++
	server := ck.LeaderId
	for {
		args := PutAppendArgs{Key: key, Value: value, Args: ck.args()}
		if op == "PUT" {
			args.Op = PutOp
		} else {
			args.Op = AppendOp
		}
		var reply PutAppendReply
		log.Printf("[ClientSend PUTAPPEND]From ClientId %d, RequesetId %d, To Server %d, key : %v, value : %v, Opreation : %v",
			ck.clientId, ck.requestId, server, key, value, op)

		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrTimeout {
			server = ck.nextLeader(server)
			continue
		}
		if ok && reply.Err == OK {
			ck.LeaderId = server
			return
		}
		//  reply.Err==ErrWrongLeader
		server = ck.nextLeader(server)
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
