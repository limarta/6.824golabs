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
	dConfigure      logTopic = "CONFIGURE"
	dDecode         logTopic = "DECODE"
	dSnap           logTopic = "SNAP"
	dApplySnap      logTopic = "APPLYSNAP"
	dApplyPutAppend logTopic = "APPLYPUTAPPEND"
	dApplyGet       logTopic = "APPLYGET"
	dReceived       logTopic = "RECEIVED"
	dSkip           logTopic = "SKIP"
	dKilled         logTopic = "KILLED"
	dPoll           logTopic = "POLL"
	dReconfig       logTopic = "RECONFIG"
	dRequest        logTopic = "REQUEST"
	dTransfer       logTopic = "TRANSFER"
	dAlive          logTopic = "ALIVE"
)
const Debug = false

func DPrintf(dTopic logTopic, format string, a ...interface{}) (n int, err error) {
	time := time.Since(debugStart).Milliseconds()
	prefix := fmt.Sprintf("%06d", time)
	if debugVerbosity == 1 {
		format = prefix + " " + string(dTopic) + " " + format
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
	Shard     int
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
	data         map[int](map[string]string)
	maxraftstate int   // snapshot if log grows this big
	dead         int32 // set by Kill()
	duplicate    map[int](map[int64]int)
	index        int
	noSnapshot   bool
}

func (kv *ShardKV) Request(cmd Op) Err {
	kv.mu.Lock()
	if _, ok := kv.duplicate[cmd.Shard][cmd.Id]; !ok {
		kv.duplicate[cmd.Shard][cmd.Id] = 0
	}
	if kv.duplicate[cmd.Shard][cmd.Id] >= cmd.ReqId {
		DPrintf(dSkip, "S[%d] (Op=%v) (kv.dup=%d) (cmd.ReqId=%d)", kv.me, cmd, kv.duplicate[cmd.Shard][cmd.Id], cmd.ReqId)
		kv.mu.Unlock()
		return OK
	}
	kv.mu.Unlock()

	index, term, isLeader := kv.rf.Start(cmd)

	if isLeader {
		if cmd.Operation == "Get" {
			DPrintf(dRequest, "S[%d-%d] starting GET (isLeader=%t) (C=%d) (reqId=%d) (shard=%d) (K=%s) (index=%d) (term=%d)",
				kv.gid, kv.me, isLeader, cmd.Id, cmd.ReqId, cmd.Shard, cmd.Key, index, term)
		} else if cmd.Operation == "Put" {
			DPrintf(dRequest, "S[%d-%d]  starting PUT (isLeader=%t) (C=%d) (reqId=%d) (shard=%d) (K=%s) (V=%s) (index=%d) (term=%d)",
				kv.gid, kv.me, isLeader, cmd.Id, cmd.ReqId, cmd.Shard, cmd.Key, cmd.Value, index, term)
		} else if cmd.Operation == "Append" {
			DPrintf(dRequest, "S[%d-%d] starting APPEND (isLeader=%t) (C=%d) (reqId=%d) (shard=%d) (K=%s) (V=%s) (index=%d) (term=%d)",
				kv.gid, kv.me, isLeader, cmd.Id, cmd.ReqId, cmd.Shard, cmd.Key, cmd.Value, index, term)
		} else if cmd.Operation == "Configure" {
			DPrintf(dRequest, "S[%d-%d] starting CONFIGURE (isLeader=%t) (C=%d) (reqId=%d) (shard=%d) (config=%v) (index=%d) (term=%d)",
				kv.gid, kv.me, isLeader, cmd.Id, cmd.ReqId, cmd.Shard, cmd.Config, index, term)

		}

		var curTerm int
		for {
			curTerm, isLeader = kv.rf.GetState()
			if curTerm > term {
				DPrintf(dRequest, "S[%d-%d] no longer leader (op=%v)", kv.gid, kv.me, cmd)
				return ErrWrongLeader
			}
			// Check if responsible for shard still
			kv.mu.Lock()
			if cmd.Operation != "Configure" && kv.config.Shards[key2shard(cmd.Key)] != kv.gid {
				DPrintf(dRequest, "S[%d-%d] not longer responsible (op=%v) (C=%d) (reqId=%d) (K=%s) (V=%s) (index=%d) (term=%d) (isLeader=%t)",
					kv.gid, kv.me, cmd.Operation, cmd.Id, cmd.ReqId, cmd.Key, cmd.Value, index, term, isLeader)
				kv.mu.Unlock()
				return ErrWrongGroup
			}
			// If configure. what to do on failures?
			if cmd.ReqId <= kv.duplicate[cmd.Shard][cmd.Id] {
				DPrintf(dRequest, "S[%d-%d] received (op=%v) (C=%d) (reqId=%d) (K=%s) (V=%s) (index=%d) (term=%d) (isLeader=%t)",
					kv.gid, kv.me, cmd.Operation, cmd.Id, cmd.ReqId, cmd.Key, cmd.Value, index, term, isLeader)
				kv.mu.Unlock()
				return OK
			}
			kv.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}
	return ErrWrongLeader
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	cmd := Op{
		Key:       args.Key,
		Id:        args.Id,
		ReqId:     args.ReqId,
		Operation: "Get",
		Shard:     key2shard(args.Key)}
	kv.mu.Lock()
	if kv.config.Shards[key2shard(cmd.Key)] != kv.gid {
		DPrintf(dGet, "S[%d-%d] reject GET (true shard gid=%d) (op=%v)", kv.gid, kv.me, kv.config.Shards[key2shard(cmd.Key)], cmd)
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()
	DPrintf(dGet, "S[%d-%d] submitting GET to Raft (op=%v)", kv.gid, kv.me, cmd)
	err := kv.Request(cmd)
	reply.Err = err
	if err == OK {
		kv.mu.Lock()
		// Check if shard is in current configuration. Change reply.Err = ErrWrongGroup
		if kv.config.Shards[key2shard(cmd.Key)] == kv.gid {
			reply.Err = err
			reply.Value = kv.data[cmd.Shard][args.Key]
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				reply.Err = ErrWrongLeader
			}
			DPrintf(dGet, "S[%d-%d] RESPONSE (isLeader belief=%t) (C=%d) (reqId=%d) (K=%s) (V=%s)", kv.gid, kv.me, isLeader, args.ReqId, args.Id, args.Key, reply.Value)
		} else {
			reply.Err = ErrWrongGroup
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	cmd := Op{
		Key:       args.Key,
		Value:     args.Value,
		Id:        args.Id,
		ReqId:     args.ReqId,
		Operation: args.Op,
		Shard:     key2shard(args.Key)}

	kv.mu.Lock()
	if kv.config.Shards[key2shard(cmd.Key)] != kv.gid {
		kv.mu.Unlock()
		DPrintf(dPutAppend, "S[%d-%d] skipped putAppend (op=%v)", kv.gid, kv.me, cmd)
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

	err := kv.Request(cmd)
	kv.mu.Lock()
	if kv.config.Shards[key2shard(cmd.Key)] == kv.gid { // Not currently correct. May fail op but now we are responsible.
		DPrintf(dPutAppend, "S[%d-%d] in (K=%s) (err=%v) (shard=%v) (res gid=%d)", kv.gid, kv.me, cmd.Key, err, key2shard(cmd.Key), kv.config.Shards[key2shard((cmd.Key))])
		reply.Err = err
	} else {
		reply.Err = ErrWrongGroup
	}

	kv.mu.Unlock()
}

func (kv *ShardKV) applier() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			if op, ok := msg.Command.(Op); ok {
				kv.mu.Lock()
				if op.Operation == "Get" {
					DPrintf(dApply, "S[%d-%d] (index=%d) (term=%d) (op=%v k=%s n=%d)",
						kv.gid, kv.me, msg.CommandIndex, msg.CommandTerm, op.Operation, op.Key, op.ReqId)
					DPrintf(dApply, "S[%d-%d] BEFORE (C=%d) (op=%s) (reqId=%d) (K=%s) (index=%d) (term=%d) (data=%v) (dup=%v)",
						kv.gid, kv.me, op.Id, op.Operation, op.ReqId, op.Key, msg.CommandIndex, msg.CommandTerm, kv.data, kv.duplicate)
				} else if op.Operation == "Put" || op.Operation == "Append" {
					DPrintf(dApply, "S[%d-%d] (index=%d) (term=%d) (op=%v k=%s v=%s n=%d)",
						kv.gid, kv.me, msg.CommandIndex, msg.CommandTerm, op.Operation, op.Key, op.Value, op.ReqId)
					DPrintf(dApply, "S[%d-%d] BEFORE (C=%d) (op=%s) (reqId=%d) (K=%s) (V=%s) (index=%d) (term=%d) (data=%v) (dup=%v)",
						kv.gid, kv.me, op.Id, op.Operation, op.ReqId, op.Key, op.Value, msg.CommandIndex, msg.CommandTerm, kv.data, kv.duplicate)
				} else if op.Operation == "Configure" {
					// DPrintf(dApply, "S[%d] BEFORE (C=%d) (op=%s) (reqId=%d) (index=%d) (term=%d) (data=%v)",
					// 	kv.me, op.Id, op.Operation, op.ReqId, msg.CommandIndex, msg.CommandTerm, kv.data)
				}

				// Make sure this is monotonic?
				if kv.index > msg.CommandIndex {
					panic("Why not increasing?")
				}
				kv.index = msg.CommandIndex
				if op.ReqId <= kv.duplicate[op.Shard][op.Id] {
					DPrintf(dApply, "S[%d-%d] skipped (C=%d) (op=%s) (reqId=%d) (K=%s)",
						kv.gid, kv.me, op.Id, op.Operation, op.ReqId, op.Key)
					kv.mu.Unlock()
					continue
				}
				if op.Operation == "Configure" {
					if kv.config.Num+1 != op.Config.Num {
						DPrintf(dApply, "S[%d-%d] CONFIGURE WRONG NUM (cur config=%v) (new config=%v) (dup=%v) (dup=%v)",
							kv.gid, kv.me, kv.config, op.Config, kv.data, kv.duplicate)
						kv.mu.Unlock()
						continue
						// panic("WRONG CONFIG NUM ")
					}
					DPrintf(dApply, "S[%d-%d] CONFIGURE (new config=%v) (cur config=%v) (data=%v) (dup=%v)", kv.gid, kv.me, op.Config, kv.config, kv.data, kv.duplicate)

					shardsToFetch := make(map[int]int) // shard -> gid
					for i, gid := range kv.config.Shards {
						// DPrintf(dApply, "S[%d-%d] (op.Config.Shards[%d]=%d) (gid=%d)", kv.gid, kv.me, i, op.Config.Shards[i], gid)
						if op.Config.Shards[i] == kv.gid && gid != op.Config.Shards[i] && gid != 0 {
							// DPrintf(dApply, "S[%d-%d] to contact (op.Config.Shards[%d]=%d) (gid=%d) (kv.config.Shards[%d]=%d)",
							// kv.gid, kv.me, i, op.Config.Shards[i], gid, i, kv.config.Shards[i])
							shardsToFetch[i] = gid
						}
					}

					DPrintf(dApply, "S[%d-%d] (shardsToFetch=%v)", kv.gid, kv.me, shardsToFetch)
					kv.noSnapshot = true
					for shard, gid := range shardsToFetch {
						DPrintf(dTransfer, "S[%d-%d] RPCing (gid=%d) for (shard=%d) for (config=%v)", kv.gid, kv.me, gid, shard, op.Config)
						kv.requestTransfer(shard, gid, op.Config)
					}
					kv.noSnapshot = false

					kv.config = op.Config
					kv.duplicate[op.Shard][op.Id] = op.ReqId // Does this line need to go outside? NO!!!!!!
				} else {
					shard := key2shard(op.Key)
					if kv.config.Shards[shard] == kv.gid {
						DPrintf(dApply, "S[%d-%d] responsible (op=%s) (shard=%d) (K=%s) (V=%s) (shards=%v)",
							kv.gid, kv.me, op.Operation, shard, op.Key, op.Value, kv.config.Shards)
						if op.Operation == "Put" {
							kv.data[shard][op.Key] = op.Value
						} else if op.Operation == "Append" {
							if val, ok := kv.data[shard][op.Key]; ok {
								kv.data[shard][op.Key] = val + op.Value
							} else {
								kv.data[shard][op.Key] = op.Value
							}
						}
						kv.duplicate[shard][op.Id] = op.ReqId // Does this line need to go outside? NO!!!!!!
					} else {
						DPrintf(dApply, "S[%d-%d] NOT responsible (shard=%d) (shards=%v) (op=%v) (K=%s)", kv.gid, kv.me, shard, kv.config.Shards, op.Operation, op.Key)
						// Somehow convey that we are not responsible
					}
				}
				if op.Operation == "Get" {
					DPrintf(dApply, "S[%d-%d] AFTER (C=%d) (op=%s) (reqId=%d) (curConfig=%v) (shard=%d) (K=%s) (index=%d) (term=%d) (data=%v) (dup=%v)",
						kv.gid, kv.me, op.Id, op.Operation, op.ReqId, kv.config, op.Shard, op.Key, msg.CommandIndex, msg.CommandTerm, kv.data, kv.duplicate)
				} else if op.Operation == "Put" || op.Operation == "Append" {
					DPrintf(dApply, "S[%d-%d] AFTER (C=%d) (op=%s) (reqId=%d) (curConfig=%v) (shard=%d) (K=%s) (V=%s) (index=%d) (term=%d) (data=%v) (dup=%v)",
						kv.gid, kv.me, op.Id, op.Operation, op.ReqId, kv.config, op.Shard, op.Key, op.Value, msg.CommandIndex, msg.CommandTerm, kv.data, kv.duplicate)
				} else if op.Operation == "Configure" {
					DPrintf(dApply, "S[%d-%d] AFTER (C=%d) (op=%s) (reqId=%d) (newConfig=%v) (newData=%v) (newDup=%v)",
						kv.gid, kv.me, op.Id, op.Operation, op.ReqId, kv.config, kv.data, kv.duplicate)
				}
				kv.mu.Unlock()
			}
		} else if msg.SnapshotValid {
			// Read snapshot = data + duplicate + index
			// Set values
			kv.mu.Lock()
			DPrintf(dSnap, "S[%d-%d] SNAPSHOT (index=%d) (term=%d)", kv.gid, kv.me, msg.SnapshotIndex, msg.SnapshotTerm)
			r := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(r)
			var data map[int](map[string]string)
			var duplicate map[int](map[int64]int)
			var index int
			var config shardctrler.Config
			if d.Decode(&data) != nil || d.Decode(&duplicate) != nil || d.Decode(&index) != nil || d.Decode(&config) != nil {
				DPrintf(dDecode, "S[%d-%d] ERROR", kv.me)
			} else {
				kv.data = data
				kv.duplicate = duplicate
				kv.index = index
				kv.config = config
				DPrintf(dDecode, "S[%d-%d] (index=%d) (data=%v) (dup=%v) (config=%v)", kv.gid, kv.me, index, data, duplicate, config)
			}

			kv.mu.Unlock()
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (kv *ShardKV) requestTransfer(shard int, gid int, config shardctrler.Config) {
	servers := kv.config.Groups[gid]
	DPrintf(dTransfer, "S[%d-%d] RPCing (gid=%d) (shard=%d) (server=%v) (config=%v)", kv.gid, kv.me, gid, shard, servers, config)
	args := TransferArgs{Shard: shard, Config: config}
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
					DPrintf(dTransfer, "S[%d-%d] RECEIVED from S[%d-%d] (shard=%d) (newData=%v) (newDup=%v)",
						kv.gid, kv.me, gid, si, shard, reply.Data, reply.Duplicate)
					kv.data[shard] = reply.Data
					kv.duplicate[shard] = reply.Duplicate
					transferred = true
					break
				} else if reply.Err == ErrTransfer {
					// Go to next one
				}
			}
		}
		time.Sleep(1 * time.Millisecond) // Problematic
	}

}

func (kv *ShardKV) Transfer(args *TransferArgs, reply *TransferReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Config.Num > kv.config.Num {
		DPrintf(dTransfer, "S[%d-%d] does NOT have (configNum=%d) (shard=%v)", kv.gid, kv.me, args.Config.Num, args.Shard)
		reply.Err = ErrTransfer
		return
	}
	data := make(map[string]string)
	duplicate := make(map[int64]int)
	for k, v := range kv.data[args.Shard] {
		data[k] = v
	}
	for k, v := range kv.duplicate[args.Shard] {
		duplicate[k] = v
	}
	// Copy over duplication table as well!
	reply.Err = OK
	reply.Data = data
	reply.Duplicate = duplicate
}

func (kv *ShardKV) snapshot() {
	if kv.maxraftstate > 0 {
		for !kv.killed() {
			kv.mu.Lock()

			if kv.rf.RaftStateSize() >= kv.maxraftstate && !kv.noSnapshot {
				DPrintf(dSnap, "S[%d-%d] (snapshot index=%d) (raftStateSize=%d) (max=%d) (data=%v) (duplicate=%v) (config=%v)",
					kv.gid, kv.me, kv.index, kv.rf.RaftStateSize(), kv.maxraftstate, kv.data, kv.duplicate, kv.config)
				// Snapshot = kv.data + kv.duplicate + index
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.data)
				e.Encode(kv.duplicate)
				e.Encode(kv.index)
				e.Encode(kv.config)
				// May need to encode config, gid, clerks?
				snapshot := w.Bytes()
				kv.rf.Snapshot(kv.index, snapshot)
			}
			kv.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) poll() {
	term := 0
	for !kv.killed() {
		kv.mu.Lock()
		cur_term, isLeader := kv.rf.GetState()
		DPrintf(dPoll, "S[%d-%d] still alive (isLeader=%t)", kv.gid, kv.me, isLeader)
		if cur_term > term || !isLeader {
			term = cur_term
			// DPrintf(dPoll, "S[%d-%d] NOT (curConfig=%v) (isLeader=%t)", kv.gid, kv.me, kv.config, isLeader)
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		DPrintf(dPoll, "S[%d-%d] (curConfig=%v) (isLeader=%t)", kv.gid, kv.me, kv.config, isLeader)
		nextConfigNum := kv.config.Num + 1
		kv.mu.Unlock()
		newConfig := kv.shardclerk.Query(nextConfigNum)
		kv.mu.Lock()
		if newConfig.Num != nextConfigNum {
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		DPrintf(dPoll, "S[%d-%d] (unconfigured=%v)", kv.gid, kv.me, newConfig)
		op := Op{Operation: "Configure",
			Id:     -1,
			ReqId:  nextConfigNum,
			Config: newConfig,
			Shard:  -1}
		DPrintf(dPoll, "S[%d-%d] (op=%v)", kv.gid, kv.me, op)
		kv.mu.Unlock()
		err := kv.Request(op)
		// Check whether we are still leader at this point
		if err == OK {
			kv.mu.Lock()
			DPrintf(dPoll, "S[%d-%d] OK (curConfig=%v)", kv.gid, kv.me, kv.config)
			kv.mu.Unlock()
		} else {
			DPrintf(dPoll, "HElp")
		}
		time.Sleep(100 * time.Millisecond)
	}

}

// func (kv *ShardKV) alive() {
// 	for {
// 		time.Sleep(500 * time.Millisecond)
// 		term, isLeader := kv.rf.GetState()
// 		DPrintf(dAlive, "S[%d-%d] (isLeader=%t) (term=%d)", kv.gid, kv.me, isLeader, term)
// 		kv.mu.Lock()
// 		DPrintf(dAlive, "S[%d-%d] * (isLeader=%t) (term=%d)", kv.gid, kv.me, isLeader, term)
// 		kv.mu.Unlock()
// 	}
// }

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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make(map[int](map[string]string))
	for i := -1; i < shardctrler.NShards; i++ {
		kv.data[i] = make(map[string]string)
	}
	// TODO: Add tables to 1-NShard and for config (-1)
	kv.duplicate = make(map[int](map[int64]int), 0)
	for i := -1; i < shardctrler.NShards; i++ {
		kv.duplicate[i] = make(map[int64]int)
	}
	// TODO: Add tables for 1-NShard and for config (-1)
	kv.index = 0
	kv.config = shardctrler.Config{Num: 0}

	// You may need initialization code here.
	go kv.applier()
	go kv.snapshot()
	go kv.poll()
	// go kv.alive()
	// go kv.reconfigure()

	return kv

}
