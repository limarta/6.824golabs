package kvraft

import (
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
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
	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	return level
}

const (
	dAppend    logTopic = "APPEND"
	dPut       logTopic = "PUT"
	dGet       logTopic = "GET"
	dApply     logTopic = "APPLY"
	dPutAppend logTopic = "PUTAPPEND"
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
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	data         map[string]string
	duplicate    map[int64]int
}

func (kv *KVServer) Request(cmd Op) Err {
	kv.mu.Lock()
	if _, ok := kv.duplicate[cmd.Id]; !ok {
		kv.duplicate[cmd.Id] = 0
	}
	if kv.duplicate[cmd.Id] >= cmd.ReqId {
		kv.mu.Unlock()
		return OK
	}
	kv.mu.Unlock()

	index, term, isLeader := kv.rf.Start(cmd)

	if isLeader {
		if cmd.Operation == "Get" {
			DPrintf(dGet, "S[%d] (C=%d) (reqId=%d) (key=%s) (index=%d) (term=%d) (isLeader=%t)",
				kv.me, cmd.Id, cmd.ReqId, cmd.Key, index, term, isLeader)
		} else if cmd.Operation == "Put" {
			DPrintf(dPut, "S[%d] (C=%d) (reqId=%d) (key=%s) (value=%s) (index=%d) (term=%d) (isLeader=%t)",
				kv.me, cmd.Id, cmd.ReqId, cmd.Key, cmd.Value, index, term, isLeader)
		} else if cmd.Operation == "Append" {
			DPrintf(dAppend, "S[%d] (C=%d) (reqId=%d) (key=%s) (value=%s) (index=%d) (term=%d) (isLeader=%t)",
				kv.me, cmd.Id, cmd.ReqId, cmd.Key, cmd.Value, index, term, isLeader)
		}

		for {
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

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	cmd := Op{
		Key:       args.Key,
		Id:        args.Id,
		ReqId:     args.ReqId,
		Operation: "Get"}
	err := kv.Request(cmd)
	reply.Err = err
	if err == OK {
		kv.mu.Lock()
		// if val, ok := kv.data[args.Key]; ok {
		// 	reply.Value = val
		// } else {
		// 	reply.Value = ""
		// }
		reply.Value = kv.data[args.Key]
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	cmd := Op{
		Key:       args.Key,
		Value:     args.Value,
		Id:        args.Id,
		ReqId:     args.ReqId,
		Operation: args.Op}
	err := kv.Request(cmd)
	reply.Err = err
}

func (kv *KVServer) applier() {
	for {
		msg := <-kv.applyCh
		if op, ok := msg.Command.(Op); ok {
			kv.mu.Lock()
			if op.Operation == "Get" {
				DPrintf(dApply, "S[%d] (C=%d) (op=%s) (K=%s) (index=%d) (term=%d)",
					kv.me, op.Id, op.Operation, op.Key, msg.CommandIndex, msg.CommandTerm)
			} else {
				DPrintf(dApply, "S[%d] (C=%d) (op=%s) (K=%s) (V=%s) (index=%d) (term=%d)",
					kv.me, op.Id, op.Operation, op.Key, op.Value, msg.CommandIndex, msg.CommandTerm)
			}

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
			// fmt.Println(kv.data)
			kv.mu.Unlock()
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.duplicate = make(map[int64]int)

	// You may need initialization code here.
	go kv.applier()

	return kv
}
