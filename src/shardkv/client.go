package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"

	mathrand "math/rand"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
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

type Clerk struct {
	sm        *shardctrler.Clerk
	config    shardctrler.Config
	make_end  func(string) *labrpc.ClientEnd
	cid       int64
	commandId int64
	leaderId  map[int]int
}

const RetryInterval = 100 * time.Millisecond

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		sm:        shardctrler.MakeClerk(ctrlers),
		make_end:  make_end,
		leaderId:  make(map[int]int),
		cid:       nrand(),
		commandId: 0,
	}
	ck.config = ck.sm.Query(-1)
	return ck
}

func (ck *Clerk) args() Args {
	return Args{ClientId: ck.cid, CommandId: ck.commandId}
}

func (ck *Clerk) Command(req *CommandArgs) string {
	req.Args = ck.args()
	for {
		shard := key2shard(req.Key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if _, ok := ck.leaderId[gid]; !ok {
				ck.leaderId[gid] = 0
			}

			oldLeaderId := ck.leaderId[gid]
			newLeaderId := oldLeaderId

			for {
				var reply CommandReply
				ok := ck.make_end(servers[newLeaderId]).Call("ShardKV.Command", req, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.leaderId[gid] = newLeaderId
					ck.cid++
					return reply.Value
				} else if ok && reply.Err == ErrWrongGroup {
					break
				} else {
					// ErrWrongLeader or ErrTimeout
					newLeaderId = (newLeaderId + 1) % len(servers)
					if newLeaderId == oldLeaderId {
						break
					}
					continue
				}
			}
		}
		time.Sleep(RetryInterval)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Get(key string) string {
	return ck.Command(&CommandArgs{Key: key, Op: GetOp})
}

func (ck *Clerk) Put(key string, value string) {
	ck.Command(&CommandArgs{Key: key, Value: value, Op: PutOp})
}

func (ck *Clerk) Append(key string, value string) {
	ck.Command(&CommandArgs{Key: key, Value: value, Op: AppendOp})
}
