package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"

	mathrand "math/rand"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	cid       int64
	commandId int64
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

func (ck *Clerk) args() Args {
	return Args{ClientId: ck.cid, CommandId: ck.commandId}
}

func (ck *Clerk) nextLeader(leaderId int) int {
	return (leaderId + 1) % len(ck.servers)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.cid = nrand()
	ck.leaderId = NServerRand(len(servers))
	return ck
}

const RetryInterval = 100 * time.Millisecond

func (ck *Clerk) Command(req *CommandArgs) Config {
	req.Args = ck.args()
	server := ck.leaderId

	for {
		var reply CommandReply
		ok := ck.servers[server].Call("ShardCtrler.Command", req, &reply)
		if !ok || reply.Err == ErrTimeout {
			server = ck.nextLeader(server)
			continue
		}
		if reply.Err == ErrWrongLeader {
			time.Sleep(RetryInterval)
			server = ck.nextLeader(server)
			continue
		}
		// ok == true && Err==OK
		ck.leaderId = server
		ck.commandId++
		return reply.Config
	}
}

func (ck *Clerk) Query(num int) Config {
	args := &CommandArgs{Num: num, Op: QueryOp}
	return ck.Command(args)
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &CommandArgs{Servers: servers, Op: JoinOp}
	ck.Command(args)
}

func (ck *Clerk) Leave(gids []int) {
	args := &CommandArgs{GIDs: gids, Op: LeaveOp}
	ck.Command(args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &CommandArgs{Shard: shard, GID: gid, Op: MoveOp}
	ck.Command(args)
}
