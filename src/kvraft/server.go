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
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	log          []Op
	data         map[string]string

	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	cmd := Op{Key: args.Key, Operation: "Get"}
	index, term, isLeader := kv.rf.Start(cmd)
	DPrintf(dGet, "S[%d] (key=%s) (index=%d) (term=%d) (isLeader=%t)", kv.me, args.Key, index, term, isLeader)
	if isLeader {
		for {
			kv.mu.Lock()
			if index >= len(kv.log)-1 {
				if val, ok := kv.data[args.Key]; ok {
					DPrintf(dGet, "S[%d] retrieved (key=%s) (value=%s)", kv.me, args.Key, val)
					reply.Err = OK
					reply.Value = val
					//do something here
				} else {
					DPrintf(dGet, "S[%d] no key (key=%s)", kv.me, args.Key)
					reply.Err = ErrNoKey
				}
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
		}
	} else {
		reply.Err = ErrWrongLeader
	}
	// Need to know if leader? Start()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	cmd := Op{Key: args.Key, Value: args.Value, Operation: args.Op}
	index, term, isLeader := kv.rf.Start(cmd)
	if isLeader {
		DPrintf(dPutAppend, "S[%d] is leader (op=%v) (key=%s) (value=\"%s\")", kv.me, args.Op, args.Key, args.Value)
		for {
			kv.mu.Lock()
			if index <= len(kv.log)-1 {
				if args.Op == "Put" {
					DPrintf(dPut, "S[%d] (index=%d) (term=%d) (isLeader=%t)", kv.me, index, term, isLeader)
					reply.Err = OK
					kv.data[args.Key] = args.Value
					DPrintf(dPut, "S[%d] (new map=%v)", kv.me, kv.data)
				} else {
					DPrintf(dAppend, "S[%d] (index=%d) (term=%d) (isLeader=%t)", kv.me, index, term, isLeader)
					if val, ok := kv.data[args.Key]; ok {
						kv.data[args.Key] = val + args.Value
					} else {
						kv.data[args.Key] = args.Value
					}
					DPrintf(dAppend, "S[%d] (new map=%v)", kv.me, kv.data)
					reply.Err = OK
				}
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
			// time.Sleep(1000)
			time.Sleep(3)
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) readApplies() {
	for {
		msg := <-kv.applyCh
		if r, ok := msg.Command.(Op); ok {
			kv.mu.Lock()
			kv.log = append(kv.log, r)
			if r.Operation == "Get" {
				DPrintf(dApply, "S[%d] (op=%s) (K=%s) (index=%d) (new log=%v)", kv.me, r.Operation, r.Key, msg.CommandIndex, kv.log)

			} else if r.Operation == "Put" {
				DPrintf(dApply, "S[%d] (op=%s) (K=%s) (V=%s) (index=%d) (new log=%v)", kv.me, r.Operation, r.Key, r.Value, msg.CommandIndex, kv.log)

			} else if r.Operation == "Append" {
				DPrintf(dApply, "S[%d] (op=%s) (K=%s) (V=%s) (index=%d) (new log=%v)", kv.me, r.Operation, r.Key, r.Value, msg.CommandIndex, kv.log)
			}
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
	kv.log = make([]Op, 0)
	kv.log = append(kv.log, Op{})
	kv.data = make(map[string]string)

	// You may need initialization code here.
	go kv.readApplies()

	return kv
}
