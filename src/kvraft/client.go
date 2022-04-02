package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	leader  int
	id      int64
	reqId   int
	// You will have to modify this struct.
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
	ck.leader = 0
	ck.id = nrand()
	ck.reqId = 0
	// You'll have to add code here.
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
func (ck *Clerk) Get(key string) string {
	// Find the server that is the leader?
	ck.reqId += 1
	args := GetArgs{Key: key,
		Id:    ck.id,
		ReqId: ck.reqId}
	reply := GetReply{}
	ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
	if ok && reply.Err == OK {
		return reply.Value
	} else if ok && reply.Err == ErrNoKey {
		return ""
	}

	i := 0
	for {
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			ck.leader = i
			return reply.Value
		} else if ok && reply.Err == ErrNoKey {
			return ""
		}
		i = (i + 1) % len(ck.servers)
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
	// You will have to modify this function.
	ck.reqId += 1
	args := PutAppendArgs{Key: key,
		Value: value,
		Id:    ck.id,
		ReqId: ck.reqId,
		Op:    op}
	reply := PutAppendReply{}
	ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
	if ok && reply.Err == OK {
		return
	}

	i := 0
	for {
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			ck.leader = i
			return
		}
		i = (i + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
