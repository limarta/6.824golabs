package shardctrler

import (
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
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	configs   []Config // indexed by config num
	duplicate map[int64]int
	index     int
}

type Op struct {
	Operation string
	Id        int64
	ReqId     int
	Servers   map[int][]string // new GID -> servers mappings // Join
	GIDs      []int            // Leave
	Shard     int              // Move
	GID       int              // Move
	Num       int              // Query
}

type logTopic string

const (
	dApply   logTopic = "APPLY"
	dRequest logTopic = "REQUEST"
	dMove    logTopic = "MOVE"
	dQuery   logTopic = "QUERY"
	dJoin    logTopic = "JOIN"
	dLeave   logTopic = "LEAVE"
	dBalance logTopic = "BALANCE"
)

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
	fmt.Println("KV VERBOSITY: ", debugVerbosity)
	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	return level
}

func DPrintf(dTopic logTopic, format string, a ...interface{}) (n int, err error) {
	// time := time.Since(debugStart).Microseconds()
	// prefix := fmt.Sprintf("%06d %-7v ", time, string(dTopic))
	format = string(dTopic) + " " + format
	if debugVerbosity == 1 {
		log.Printf(format, a...)
	}
	return
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	cmd := Op{
		Operation: "Join",
		Id:        args.Id,
		ReqId:     args.ReqId,
		Servers:   args.Servers,
	}
	err := sc.Request(cmd)
	reply.WrongLeader = (err != OK)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	cmd := Op{
		Operation: "Leave",
		Id:        args.Id,
		ReqId:     args.ReqId,
		GIDs:      args.GIDs,
	}

	err := sc.Request(cmd)
	reply.WrongLeader = (err != OK)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	cmd := Op{
		Operation: "Move",
		Id:        args.Id,
		ReqId:     args.ReqId,
		Shard:     args.Shard,
		GID:       args.GID,
	}

	err := sc.Request(cmd)
	reply.WrongLeader = (err != OK)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	cmd := Op{
		Operation: "Query",
		Id:        args.Id,
		ReqId:     args.ReqId,
		Num:       args.Num,
	}

	sc.mu.Lock()
	if args.Num > 0 && args.Num < len(sc.configs) {
		reply.Config = sc.configs[args.Num]
		_, isLeader := sc.rf.GetState()
		if !isLeader {
			reply.WrongLeader = true
		}
		// fmt.Println("SKIPPED ", args.Num)
		sc.mu.Unlock()
		return
	} else if args.Num == -1 {
		reply.Config = sc.configs[len(sc.configs)-1]
		_, isLeader := sc.rf.GetState()
		if !isLeader {
			reply.WrongLeader = true
			sc.mu.Unlock()
			return
		}
		// fmt.Println("SKIPPED ", args.Num)

	}
	sc.mu.Unlock()

	err := sc.Request(cmd)
	reply.WrongLeader = (err != OK)
	if err == OK {
		sc.mu.Lock()
		if args.Num == -1 || args.Num > len(sc.configs)-1 {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		_, isLeader := sc.rf.GetState()
		if !isLeader {
			reply.WrongLeader = true
		} else {
		}
		sc.mu.Unlock()
	}
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
		if cmd.Operation == "Query" {
			DPrintf(dRequest, "S[%d] (op=%s) (config_id=%d)", sc.me, cmd.Operation, cmd.Num)
		} else if cmd.Operation == "Join" {
			DPrintf(dRequest, "S[%d] (op=%s) (servers=%v)", sc.me, cmd.Operation, cmd.Servers)
		} else if cmd.Operation == "Leave" {
			DPrintf(dRequest, "S[%d] (op=%s) (GIDs=%v)", sc.me, cmd.Operation, cmd.GIDs)
		} else if cmd.Operation == "Move" {
			DPrintf(dRequest, "S[%d] (op=%s) (Shard=%d) (GID=%d)", sc.me, cmd.Operation, cmd.Shard, cmd.GID)
		}
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
				DPrintf(dApply, "S[%d] (op=%v)\n", sc.me, op)

				// if kv.index > msg.CommandIndex {
				// 	panic("Why not increasing?")
				// }
				sc.index = msg.CommandIndex

				if op.ReqId > sc.duplicate[op.Id] {
					config := sc.configs[len(sc.configs)-1]
					newConfig := Config{Num: config.Num + 1}
					DPrintf(dApply, "S[%d] (newConfig #=%d)", sc.me, newConfig.Num)
					if op.Operation == "Join" {
						newGroups := make(map[int][]string)
						for k, v := range config.Groups {
							newGroups[k] = v
						}
						for gid := range op.Servers {
							newGroups[gid] = append(newGroups[gid], op.Servers[gid]...)
						}

						newShards := config.Shards
						GIDs := make([]int, 0)
						for gid := range newGroups {
							GIDs = append(GIDs, gid)
						}
						sort.Ints(GIDs)
						for i, gid := range newShards {
							if gid == 0 {
								newShards[i] = GIDs[0]
							}
						}
						// Check for 0; assign

						newConfig.Groups = newGroups
						newConfig.Shards = sc.rebalance(newShards, GIDs)
						DPrintf(dJoin, "S[%d] (newGroups=%v) (newShards=%v)", sc.me, newGroups, newConfig.Shards)
					} else if op.Operation == "Leave" {
						newGroups := make(map[int][]string)
						for k, v := range config.Groups {
							newGroups[k] = v
						}

						for _, gid := range op.GIDs {
							delete(newGroups, gid)
						}
						GIDs := make([]int, 0)
						for gid := range newGroups {
							GIDs = append(GIDs, gid)
						}
						sort.Ints(GIDs)
						DPrintf(dLeave, "S[%d] (newGroups=%v) (new GIDs=%v)", sc.me, newGroups, GIDs)

						newShards := config.Shards
						for i, gid := range newShards {
							found := false
							for _, del_gid := range GIDs {
								if del_gid == gid {
									found = true
									break
								}
							}
							if !found {
								if len(GIDs) == 0 {
									newShards[i] = 0
								} else {
									newShards[i] = GIDs[0]
								}
							}
						}

						newConfig.Groups = newGroups
						newConfig.Shards = sc.rebalance(newShards, GIDs)
						DPrintf(dLeave, "S[%d] (newGroups=%v) (newShards=%v)", sc.me, newGroups, newConfig.Shards)
					} else if op.Operation == "Move" {
						newConfig.Shards = config.Shards
						newConfig.Shards[op.Shard] = op.GID
					}

					if op.Operation != "Query" {
						sc.configs = append(sc.configs, newConfig)
					}
					sc.duplicate[op.Id] = op.ReqId
				}
				DPrintf(dApply, "S[%d] (new configs %v)", sc.me, sc.configs)
				sc.mu.Unlock()
			}
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func (sc *ShardCtrler) rebalance(shards [NShards]int, GIDs []int) [NShards]int {
	DPrintf(dBalance, "S[%d] (shards=%v) (GIDs=%v)", sc.me, shards, GIDs)
	for {
		freq := make(map[int][]int)
		min := NShards + 1
		min_gid := -1
		max := -1
		max_gid := -1
		for i, gid := range shards {
			freq[gid] = append(freq[gid], i)
		}
		for _, gid := range GIDs {
			if max < len(freq[gid]) {
				max_gid = gid
				max = len(freq[gid])
			}
			if min > len(freq[gid]) {
				min_gid = gid
				min = len(freq[gid])
			}
		}
		// DPrintf(dBalance, "S[%d] (freq=%v) (min=%d min_gid=%d) (max=%d max_gid=%d)", sc.me, freq, min, min_gid, max, max_gid)
		if max-min > 1 {
			index := freq[max_gid][len(freq[max_gid])-1]
			// DPrintf(dBalance, "S[%d] (swap index=%d)", sc.me, index)
			shards[index] = min_gid
		} else {
			break
		}
	}
	DPrintf(dBalance, "S[%d] (new_balance=%v)", sc.me, shards)
	return shards
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

	sc.index = 0
	sc.duplicate = make(map[int64]int)

	go sc.applier()
	return sc
}
