package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type logTopic string

var debugVerbosity int
var debugStart time.Time

func setVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	debugVerbosity = level
	fmt.Println("SHARD VERBOSITY: ", debugVerbosity)
	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	return level
}

const (
	dAppend         logTopic = "APPEND"
	dPut            logTopic = "PUT"
	dGet            logTopic = "GET"
	dApply          logTopic = "APPLY"
	dPutAppend      logTopic = "PUTAPPEND"
	dDecode         logTopic = "DECODE"
	dSnap           logTopic = "SNAP"
	dApplySnap      logTopic = "APPLYSNAP"
	dApplyPutAppend logTopic = "APPLYPUTAPPEND"
	dApplyGet       logTopic = "APPLYGET"
	dReceived       logTopic = "RECEIVED"
	dSkip           logTopic = "SKIP"
)
const Debug = false

func DPrintf(dTopic logTopic, format string, a ...interface{}) (n int, err error) {
	// time := time.Since(debugStart).Microseconds()
	// prefix := fmt.Sprintf("%06d %-7v ", time, string(dTopic))
	format = string(dTopic) + " " + format
	log.Printf(format, a...)
	return
}

type Op struct {
	Key       string
	Value     string
	Operation string
	Id        int64
	ReqId     int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	shardclerk   *shardctrler.Clerk
	config       shardctrler.Config
	maxraftstate int   // snapshot if log grows this big
	dead         int32 // set by Kill()
	data         map[string]string
	duplicate    map[int64]int
	index        int

	// Your definitions here.
}

func (kv *ShardKV) Request(cmd Op) Err {
	kv.mu.Lock()
	if _, ok := kv.duplicate[cmd.Id]; !ok {
		kv.duplicate[cmd.Id] = 0
	}
	if kv.duplicate[cmd.Id] >= cmd.ReqId {
		DPrintf(dSkip, "S[%d] (Op=%v) (kv.dup=%d) (cmd.ReqId=%d)", kv.me, cmd, kv.duplicate[cmd.Id], cmd.ReqId)
		kv.mu.Unlock()
		return OK
	}
	kv.mu.Unlock()

	index, term, isLeader := kv.rf.Start(cmd)

	if isLeader {
		if cmd.Operation == "Get" {
			DPrintf(dGet, "S[%d] (isLeader=%t) (C=%d) (reqId=%d) (key=%s) (index=%d) (term=%d)",
				kv.me, isLeader, cmd.Id, cmd.ReqId, cmd.Key, index, term)
		} else if cmd.Operation == "Put" {
			DPrintf(dPut, "S[%d]  (isLeader=%t) (C=%d) (reqId=%d) (key=%s) (value=%s) (index=%d) (term=%d)",
				kv.me, isLeader, cmd.Id, cmd.ReqId, cmd.Key, cmd.Value, index, term)
		} else if cmd.Operation == "Append" {
			DPrintf(dAppend, "S[%d] (isLeader=%t) (C=%d) (reqId=%d) (key=%s) (value=%s) (index=%d) (term=%d)",
				kv.me, isLeader, cmd.Id, cmd.ReqId, cmd.Key, cmd.Value, index, term)
		}

		start := time.Now()

		for !kv.killed() && time.Now().Sub(start).Seconds() < float64(2) {
			kv.mu.Lock()
			cur_term, _ := kv.rf.GetState()
			if cmd.ReqId <= kv.duplicate[cmd.Id] {
				DPrintf(dAppend, "S[%d] (C=%d) (reqId=%d) (key=%s) (value=%s) (index=%d) (term=%d) (isLeader=%t)",
					kv.me, cmd.Id, cmd.ReqId, cmd.Key, cmd.Value, index, term, isLeader)
				kv.mu.Unlock()
				return OK
			} else if cur_term > term {
				kv.mu.Unlock()
				return ErrWrongLeader
			}
			kv.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}
	return ErrWrongLeader
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			if op, ok := msg.Command.(Op); ok {
				kv.mu.Lock()
				if op.Operation == "Get" {
					DPrintf(dReceived, "S[%d] (index=%d) (term=%d) (op=%v k=%s n=%d)",
						kv.me, msg.CommandIndex, msg.CommandTerm, op.Operation, op.Key, op.ReqId)
					DPrintf(dApply, "S[%d] BEFORE (C=%d) (op=%s) (reqId=%d) (K=%s) (index=%d) (term=%d) (data=%v)",
						kv.me, op.Id, op.Operation, op.ReqId, op.Key, msg.CommandIndex, msg.CommandTerm, kv.data)
				} else {
					DPrintf(dReceived, "S[%d] (index=%d) (term=%d) (op=%v k=%s v=%s n=%d)",
						kv.me, msg.CommandIndex, msg.CommandTerm, op.Operation, op.Key, op.Value, op.ReqId)
					DPrintf(dApply, "S[%d] BEFORE (C=%d) (op=%s) (reqId=%d) (K=%s) (V=%s) (index=%d) (term=%d) (data=%v)",
						kv.me, op.Id, op.Operation, op.ReqId, op.Key, op.Value, msg.CommandIndex, msg.CommandTerm, kv.data)
				}

				// Make sure this is monotonic?
				// if kv.index > msg.CommandIndex {
				// 	panic("Why not increasing?")
				// }
				kv.index = msg.CommandIndex

				if op.ReqId > kv.duplicate[op.Id] {
					if op.Operation == "Put" {
						kv.data[op.Key] = op.Value
					} else if op.Operation == "Append" {
						if val, ok := kv.data[op.Key]; ok {
							kv.data[op.Key] = val + op.Value
						} else {
							kv.data[op.Key] = op.Value
						}
					}
					kv.duplicate[op.Id] = op.ReqId
				}
				if op.Operation == "Get" {
					DPrintf(dApply, "S[%d] AFTER (C=%d) (op=%s) (reqId=%d) (K=%s) (index=%d) (term=%d) (data=%v)",
						kv.me, op.Id, op.Operation, op.ReqId, op.Key, msg.CommandIndex, msg.CommandTerm, kv.data)
				} else if op.Operation == "Put" {
					DPrintf(dApply, "S[%d] AFTER (C=%d) (op=%s) (reqId=%d) (K=%s) (V=%s) (index=%d) (term=%d) (data=%v)",
						kv.me, op.Id, op.Operation, op.ReqId, op.Key, op.Value, msg.CommandIndex, msg.CommandTerm, kv.data)
				}
			}
			// fmt.Println(kv.data)
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			// Read snapshot = data + duplicate + index
			// Set values
			kv.mu.Lock()
			DPrintf(dReceived, "S[%d] SNAPSHOT (index=%d) (term=%d)", kv.me, msg.SnapshotIndex, msg.SnapshotTerm)
			r := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(r)
			var data map[string]string
			var duplicate map[int64]int
			var index int
			if d.Decode(&data) != nil || d.Decode(&duplicate) != nil || d.Decode(&index) != nil {
				DPrintf(dDecode, "[S%d] ERROR", kv.me)
			} else {
				kv.data = data
				kv.duplicate = duplicate
				kv.index = index
				DPrintf(dDecode, "[S%d] (index=%d) (data=%v) (dup=%v)", kv.me, index, data, duplicate)
			}

			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) snapshot() {
	if kv.maxraftstate > 0 {
		for !kv.killed() {
			kv.mu.Lock()

			if kv.rf.RaftStateSize() >= kv.maxraftstate {
				DPrintf(dSnap, "S[%d] (snapshot index=%d) (raftStateSize=%d) (max=%d)", kv.me, kv.index, kv.rf.RaftStateSize(), kv.maxraftstate)
				// Snapshot = kv.data + kv.duplicate + index
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.data)
				e.Encode(kv.duplicate)
				e.Encode(kv.index)
				snapshot := w.Bytes()
				kv.rf.Snapshot(kv.index, snapshot)
			}
			kv.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) poll() {
	for !kv.killed() {
		kv.mu.Lock()
		time.Sleep(100 * time.Millisecond)
		kv.config = kv.shardclerk.Query(-1)
		kv.mu.Unlock()
	}

}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}
func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.shardclerk = shardctrler.MakeClerk(ctrlers)

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make(map[string]string)
	kv.index = 0
	kv.duplicate = make(map[int64]int)

	// You may need initialization code here.
	go kv.applier()
	go kv.snapshot()
	go kv.poll()

	return kv

}
