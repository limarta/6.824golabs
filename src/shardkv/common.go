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
	ErrTransfer    = "ErrTransfer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id    int64
	ReqId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key   string
	Id    int64
	ReqId int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type TransferArgs struct {
	ConfigNum int
}

type TransferReply struct {
	Err  Err
	Data map[string]string
}
