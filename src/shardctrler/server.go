package shardctrler

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int
	configs      []Config // indexed by config num
	duplicate    map[int64]int
	index        int
}

// type Op struct {
// 	Key       string
// 	Value     string
// }
type Op struct {
	Operation string
	Id        int64
	ReqId     int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	cmd := Op{
		Operation: "Join",
		Id:        args.Id,
		ReqId:     args.ReqId,
	}
	err := sc.Request(cmd)
	reply.Err = err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	cmd := Op{
		Operation: "Leave",
		Id:        args.Id,
		ReqId:     args.ReqId,
	}
	err := sc.Request(cmd)
	reply.Err = err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	cmd := Op{
		Operation: "Move",
		Id:        args.Id,
		ReqId:     args.ReqId,
	}
	err := sc.Request(cmd)
	reply.Err = err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	cmd := Op{
		Operation: "Query",
		Id:        args.Id,
		ReqId:     args.ReqId,
	}
	err := sc.Request(cmd)
	reply.Err = err
	// if err == OK {
	// 	kv.mu.Lock()
	// 	reply.Value = kv.data[args.Key]
	// 	_, isLeader := kv.rf.GetState()
	// 	if !isLeader {
	// 		reply.Err = ErrWrongLeader
	// 	}
	// 	kv.mu.Unlock()
	// }
}

func (sc *ShardCtrler) Request(cmd Op) Err {
	sc.mu.Lock()
	if _, ok := sc.duplicate[cmd.Id]; !ok {
		sc.duplicate[cmd.Id] = 0
	}
	if sc.duplicate[cmd.Id] >= cmd.ReqId {
		sc.mu.Unlock()
		return OK
	}
	sc.mu.Unlock()

	_, term, isLeader := sc.rf.Start(cmd)

	if isLeader {
		// 		if cmd.Operation == "Get" {
		// 			DPrintf(dGet, "S[%d] (isLeader=%t) (C=%d) (reqId=%d) (key=%s) (index=%d) (term=%d)",
		// 				kv.me, isLeader, cmd.Id, cmd.ReqId, cmd.Key, index, term)
		// 		} else if cmd.Operation == "Put" {
		// 			DPrintf(dPut, "S[%d]  (isLeader=%t) (C=%d) (reqId=%d) (key=%s) (value=%s) (index=%d) (term=%d)",
		// 				kv.me, isLeader, cmd.Id, cmd.ReqId, cmd.Key, cmd.Value, index, term)
		// 		} else if cmd.Operation == "Append" {
		// 			DPrintf(dAppend, "S[%d] (isLeader=%t) (C=%d) (reqId=%d) (key=%s) (value=%s) (index=%d) (term=%d)",
		// 				kv.me, isLeader, cmd.Id, cmd.ReqId, cmd.Key, cmd.Value, index, term)
		// 		}

		start := time.Now()

		for !sc.killed() && time.Now().Sub(start).Seconds() < float64(2) {
			sc.mu.Lock()
			cur_term, _ := sc.rf.GetState()
			if cmd.ReqId <= sc.duplicate[cmd.Id] {
				sc.mu.Unlock()
				return OK
			} else if cur_term > term {
				sc.mu.Unlock()
				return ErrWrongLeader
			}
			sc.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}
	return ErrWrongLeader
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		msg := <-sc.applyCh
		if msg.CommandValid {
			if op, ok := msg.Command.(Op); ok {
				sc.mu.Lock()

				// Make sure this is monotonic?
				// if kv.index > msg.CommandIndex {
				// 	panic("Why not increasing?")
				// }
				sc.index = msg.CommandIndex

				if op.ReqId > sc.duplicate[op.Id] {
					// if op.Operation == "" {
					// 	kv.data[op.Key] = op.Value
					// } else if op.Operation == "Append" {
					// 	if val, ok := kv.data[op.Key]; ok {
					// 		sc.data[op.Key] = val + op.Value
					// 	} else {
					// 		sc.data[op.Key] = op.Value
					// 	}
					// }
					// sc.duplicate[op.Id] = op.ReqId
				}
			}
			sc.mu.Unlock()
			// } else if msg.SnapshotValid {
			// 	// Read snapshot = data + duplicate + index
			// 	// Set values
			// 	kv.mu.Lock()
			// 	DPrintf(dReceived, "S[%d] SNAPSHOT (index=%d) (term=%d)", kv.me, msg.SnapshotIndex, msg.SnapshotTerm)
			// 	r := bytes.NewBuffer(msg.Snapshot)
			// 	d := labgob.NewDecoder(r)
			// 	var data map[string]string
			// 	var duplicate map[int64]int
			// 	var index int
			// 	if d.Decode(&data) != nil || d.Decode(&duplicate) != nil || d.Decode(&index) != nil {
			// 		DPrintf(dDecode, "[S%d] ERROR", kv.me)
			// 	} else {
			// 		kv.data = data
			// 		kv.duplicate = duplicate
			// 		kv.index = index
			// 		DPrintf(dDecode, "[S%d] (index=%d) (data=%v) (dup=%v)", kv.me, index, data, duplicate)
			// 	}

			// 	kv.mu.Unlock()
		}
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// sc.data = make(map[string]string)
	sc.index = 0
	sc.duplicate = make(map[int64]int)

	// You may need initialization code here.
	go sc.applier()
	return sc
}
