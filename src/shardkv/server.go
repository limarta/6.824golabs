package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"sort"
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
	dKilled         logTopic = "KILLED"
)
const Debug = false

func DPrintf(dTopic logTopic, format string, a ...interface{}) (n int, err error) {
	// time := time.Since(debugStart).Microseconds()
	// prefix := fmt.Sprintf("%06d %-7v ", time, string(dTopic))
	if debugVerbosity == 1 {
		format = string(dTopic) + " " + format
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key       string
	Value     string
	Operation string
	Id        int64
	ReqId     int
	Config    shardctrler.Config
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
	data         [](map[string]string)
	maxraftstate int   // snapshot if log grows this big
	dead         int32 // set by Kill()
	duplicate    map[int64]int
	index        int
	unconfigured []shardctrler.Config

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

func (kv *ShardKV) curData() map[string]string { // Must be called with a lock
	return kv.data[len(kv.data)-1]
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	cmd := Op{
		Key:       args.Key,
		Id:        args.Id,
		ReqId:     args.ReqId,
		Operation: "Get"}
	err := kv.Request(cmd)
	reply.Err = err
	if err == OK {
		kv.mu.Lock()
		reply.Value = kv.curData()[args.Key]
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
		}
		DPrintf(dGet, "[S%d] RESPONSE (isLeader belief=%t) (C=%d) (reqId=%d) (K=%s) (V=%s)", kv.me, isLeader, args.ReqId, args.Id, args.Key, reply.Value)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	cmd := Op{
		Key:       args.Key,
		Value:     args.Value,
		Id:        args.Id,
		ReqId:     args.ReqId,
		Operation: args.Op}
	err := kv.Request(cmd)
	reply.Err = err
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			if op, ok := msg.Command.(Op); ok {
				kv.mu.Lock()
				// if op.Operation == "Get" {
				// 	DPrintf(dReceived, "S[%d] (index=%d) (term=%d) (op=%v k=%s n=%d)",
				// 		kv.me, msg.CommandIndex, msg.CommandTerm, op.Operation, op.Key, op.ReqId)
				// 	DPrintf(dApply, "S[%d] BEFORE (C=%d) (op=%s) (reqId=%d) (K=%s) (index=%d) (term=%d) (data=%v)",
				// 		kv.me, op.Id, op.Operation, op.ReqId, op.Key, msg.CommandIndex, msg.CommandTerm, kv.data)
				// } else {
				// 	DPrintf(dReceived, "S[%d] (index=%d) (term=%d) (op=%v k=%s v=%s n=%d)",
				// 		kv.me, msg.CommandIndex, msg.CommandTerm, op.Operation, op.Key, op.Value, op.ReqId)
				// 	DPrintf(dApply, "S[%d] BEFORE (C=%d) (op=%s) (reqId=%d) (K=%s) (V=%s) (index=%d) (term=%d) (data=%v)",
				// 		kv.me, op.Id, op.Operation, op.ReqId, op.Key, op.Value, msg.CommandIndex, msg.CommandTerm, kv.data)
				// }

				// Make sure this is monotonic?
				// if kv.index > msg.CommandIndex {
				// 	panic("Why not increasing?")
				// }
				kv.index = msg.CommandIndex

				if op.Operation == "Reconfig" {
					mapCopy := make(map[string]string)
					for k, v := range kv.curData() {
						mapCopy[k] = v
					}
					kv.data = append(kv.data, mapCopy)

					gidsMap := make(map[int]bool)
					for i, gid := range kv.config.Shards {
						if op.Config.Shards[i] == kv.me && gid != kv.me {
							gidsMap[gid] = true
						}
					}
					gidsToContact := make([]int, 0)
					for gid := range gidsMap {
						gidsToContact = append(gidsToContact, gid)
					}
					sort.Ints(gidsToContact)

					var wg sync.WaitGroup
					for gid := range gidsToContact {
						wg.Add(1)
						go func(g int, o Op) {
							kv.requestTransfer(g, o)
						}(gid, op)
						// Create clerk for each gid
					}
					wg.Wait()
					kv.config = op.Config
				} else {
					if op.ReqId > kv.duplicate[op.Id] {
						shard := key2shard(op.Key)
						if kv.config.Shards[shard] == kv.gid {
							if op.Operation == "Put" {
								kv.curData()[op.Key] = op.Value
							} else if op.Operation == "Append" {
								if val, ok := kv.curData()[op.Key]; ok {
									kv.curData()[op.Key] = val + op.Value
								} else {
									kv.curData()[op.Key] = op.Value
								}
							}
							kv.duplicate[op.Id] = op.ReqId // Does this line need to go outside?
							// 	// GID responsible for shard
						} else {
							// Somehow convey that we are not responsible
						}
					}
				}
				if op.Operation == "Get" {
					DPrintf(dApply, "S[%d] AFTER (C=%d) (op=%s) (reqId=%d) (K=%s) (index=%d) (term=%d) (data=%v)",
						kv.me, op.Id, op.Operation, op.ReqId, op.Key, msg.CommandIndex, msg.CommandTerm, kv.data)
				} else if op.Operation == "Put" {
					DPrintf(dApply, "S[%d] AFTER (C=%d) (op=%s) (reqId=%d) (K=%s) (V=%s) (index=%d) (term=%d) (data=%v)",
						kv.me, op.Id, op.Operation, op.ReqId, op.Key, op.Value, msg.CommandIndex, msg.CommandTerm, kv.data)
				}
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			// Read snapshot = data + duplicate + index
			// Set values
			kv.mu.Lock()
			DPrintf(dReceived, "S[%d] SNAPSHOT (index=%d) (term=%d)", kv.me, msg.SnapshotIndex, msg.SnapshotTerm)
			r := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(r)
			var data []map[string]string
			var duplicate map[int64]int
			var index int
			// var config shardctrler.Config
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

func (kv *ShardKV) requestTransfer(gid int, op Op) {
	servers := kv.config.Groups[gid]
	args := TransferArgs{op.Config.Num}
	transferred := false
	for !kv.killed() && !transferred {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply TransferReply
			kv.mu.Unlock()
			ok := srv.Call("ShardKV.Transfer", &args, &reply)
			kv.mu.Lock()
			if ok {
				if reply.Err == OK {
					for k, v := range reply.Data {
						if op.Config.Shards[key2shard(k)] == kv.gid && kv.config.Shards[key2shard(k)] != kv.gid {
							kv.curData()[k] = v
						}
					}
					transferred = true
					break
				} else if reply.Err == ErrTransfer {

				}
			}
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
				// May need to encode config, gid, clerks?
				snapshot := w.Bytes()
				kv.rf.Snapshot(kv.index, snapshot)
			}
			kv.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) Transfer(args *TransferArgs, reply *TransferReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.ConfigNum > len(kv.data) {
		reply.Err = ErrTransfer
		return
	}
	data := make(map[string]string)
	for k, v := range kv.data[args.ConfigNum] {
		data[k] = v
	}
	reply.Err = OK
	reply.Data = data
}

func (kv *ShardKV) reconfigure() {
	// Commit to raft
	kv.mu.Unlock()
	for !kv.killed() {
		kv.mu.Lock()
		if len(kv.unconfigured) > 0 {
			op := Op{Operation: "Configure", Id: -1, ReqId: kv.unconfigured[0].Num, Config: kv.unconfigured[0]}
			err := kv.Request(op)
			kv.mu.Unlock()
			// Check whether we are still leader at this point
			if err == OK {
				kv.mu.Lock()
				kv.unconfigured = kv.unconfigured[1:]
				kv.mu.Unlock()
				continue
			} else {
				// Do again?
			}
		}
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}

}

func (kv *ShardKV) poll() {
	term := 0
	for !kv.killed() {
		kv.mu.Lock()
		cur_term, isLeader := kv.rf.GetState()
		if cur_term > term || !isLeader {
			kv.unconfigured = make([]shardctrler.Config, 0)
			term = cur_term
			kv.mu.Unlock()
			continue
		}
		nextConfigNum := -1
		if len(kv.unconfigured) == 0 {
			nextConfigNum = kv.config.Num + 1
		} else {
			nextConfigNum = kv.unconfigured[len(kv.unconfigured)-1].Num + 1
		}
		kv.unconfigured = append(kv.unconfigured, kv.shardclerk.Query(nextConfigNum))
		kv.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
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
	DPrintf(dKilled, "S[%d-%d] killed\n", kv.gid, kv.me)
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
	kv.shardclerk = shardctrler.MakeClerk(kv.ctrlers)

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make([]map[string]string, 0)
	kv.data = append(kv.data, make(map[string]string))
	kv.index = 0
	kv.duplicate = make(map[int64]int)
	kv.config = shardctrler.Config{Num: 0}
	kv.unconfigured = make([]shardctrler.Config, 0)

	// You may need initialization code here.
	go kv.applier()
	go kv.snapshot()
	go kv.poll()
	go kv.reconfigure()

	return kv

}
