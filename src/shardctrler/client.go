package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leader int
	id     int64
	reqId  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.leader = 0
	ck.id = nrand()
	ck.reqId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.reqId += 1
	args := &QueryArgs{}
	args.Num = num
	args.Id = ck.id
	args.ReqId = ck.reqId
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.reqId += 1
	args := &JoinArgs{}
	args.Servers = servers
	args.Id = ck.id
	args.ReqId = ck.reqId

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.reqId += 1
	args := &LeaveArgs{}
	args.GIDs = gids
	args.Id = ck.id
	args.ReqId = ck.reqId
	fmt.Println("Leave ", args)

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.reqId += 1
	args := &MoveArgs{}
	args.Shard = shard
	args.GID = gid
	args.Id = ck.id
	args.ReqId = ck.reqId
	fmt.Println("Move ", args)

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
