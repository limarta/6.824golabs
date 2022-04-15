package shardctrler

import (
	"fmt"
	"sort"
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
)

func DPrintf(dTopic logTopic, format string, a ...interface{}) (n int, err error) {
	// time := time.Since(debugStart).Microseconds()
	// prefix := fmt.Sprintf("%06d %-7v ", time, string(dTopic))
	format = string(dTopic) + " " + format
	fmt.Printf(format, a...)
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

	// fmt.Printf("(Op: %s) (Id=%d) (ReqId=%d) (GIDs=%v)\n", cmd.Operation, cmd.Id, cmd.ReqId, cmd.GIDs)
	err := sc.Request(cmd)
	// fmt.Println("Error: ", err)
	reply.WrongLeader = (err != OK)
	// fmt.Println("isWrongLeader ", reply.WrongLeader)
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

	err := sc.Request(cmd)
	reply.WrongLeader = (err != OK)
	if err == OK {
		sc.mu.Lock()
		fmt.Println("Before query configs: ", sc.configs)
		if args.Num == -1 || args.Num > len(sc.configs)-1 {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		_, isLeader := sc.rf.GetState()
		if !isLeader {
			reply.WrongLeader = true
		} else {
			fmt.Printf("S[%d] Query response %v\n", sc.me, reply.Config)
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
		DPrintf(dRequest, "S[%d] (cmd=%v)\n", sc.me, cmd)
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
					if op.Operation == "Join" {
						DPrintf(dJoin, "S[%d]", sc.me)
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
						sc.rebalance(newShards, GIDs)

						newConfig.Groups = newGroups
						newConfig.Shards = newShards
					} else if op.Operation == "Leave" {
						newGroups := make(map[int][]string)
						for k, v := range config.Groups {
							newGroups[k] = v
						}

						for gid := range op.GIDs {
							delete(newGroups, gid)
						}
						GIDs := make([]int, 0)
						for gid := range newGroups {
							GIDs = append(GIDs, gid)
						}
						sort.Ints(GIDs)

						newShards := config.Shards
						for i, gid := range newShards {
							found := false
							for del_gid := range newShards {
								if del_gid == gid {
									found = true
									break
								}
							}
							if found {
								newShards[i] = GIDs[0]
							}
						}

						sc.rebalance(newShards, GIDs)
						newConfig.Groups = newGroups
						newConfig.Shards = newShards
					} else if op.Operation == "Move" {
						newConfig.Shards = config.Shards
						newConfig.Shards[op.Shard] = op.GID
					}

					if op.Operation != "Query" {
						sc.configs = append(sc.configs, newConfig)
					}
					sc.duplicate[op.Id] = op.ReqId
				}
				fmt.Printf("S[%d] Configurations: %v\n", sc.me, sc.configs)
				sc.mu.Unlock()
			}
		}
	}
}

func (sc *ShardCtrler) rebalance(shards [NShards]int, GIDs []int) {
	for {
		freq := make(map[int][]int)
		min := NShards + 1
		min_gid := -1
		max := -1
		max_gid := -1
		for i, gid := range shards {
			freq[gid] = append(freq[gid], i)
		}
		for gid := range GIDs {
			if max < len(freq[gid]) {
				max_gid = gid
				max = len(freq[gid])
			}
			if min > len(freq[gid]) {
				min_gid = gid
				min = len(freq[gid])
			}
		}
		if max_gid-min_gid > 1 {
			index := freq[max_gid][len(freq[max_gid])-1]
			shards[index] = min_gid
		} else {
			break
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

	sc.index = 0
	sc.duplicate = make(map[int64]int)

	go sc.applier()
	return sc
}
