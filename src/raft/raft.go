package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

type Job int

const (
	Follower Job = iota
	Leader
	Candidate
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	job         Job
	isLeader    bool
	term        int
	nextIndex   []int
	matchIndex  []int // Initialized to 0
	commitIndex int
	lastApplied int
	shouldReset bool
	logs        []Log
	votedFor    int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.term
	isleader = rf.isLeader
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type Log struct {
	Term    int
	Command interface{}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//Check if leader, follower, candidate?
	rf.mu.Lock()
	DPrintf(dReqVote, "[S%d] received VoteRequest from [S%d] for term %d\n", rf.me, args.CandidateId, args.Term)
	// TODO: Must check Complete Leader condition. Incomplete right now.
	if rf.term > args.Term { // Candidate is old. Reject.
		DPrintf(dReqVote, "[S%d] (term = %d) old RequestVote from [S%d] (term = %d)\n", rf.me, rf.term, args.CandidateId, args.Term)
		reply.Term = rf.term
		reply.VoteGranted = false
	} else if rf.term < args.Term {
		DPrintf(dReqVote, "[S%d] (term = %d) valid RequestVote from [S%d] (term = %d)\n", rf.me, rf.term, args.CandidateId, args.Term)
		rf.term = args.Term
		rf.job = Follower
		rf.isLeader = false
		rf.votedFor = args.CandidateId
		rf.shouldReset = true
		DPrintf(dReqVote, "[S%d] accepted requestVote from [S%d], new term %d job %d isLeader %t votedFor %d shouldReset %t\n", rf.me, args.Term, rf.term, rf.job, rf.isLeader, rf.votedFor, rf.shouldReset)
		reply.Term = rf.term
		reply.VoteGranted = true
	} else {
		reply.Term = rf.term
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Make sure server has been initialized before others servers can successfully append/get votes

	rf.mu.Lock()
	if rf.term > args.Term { // Received AppendEntries from old leader
		reply.Success = false
		reply.Term = rf.term // For old leader to update itself
		rf.mu.Unlock()
		return
	} else if rf.job == Leader && rf.term < args.Term { // Server is outdated. Note: What about rf.term == args.term?
		rf.term = args.Term
		rf.job = Follower
		rf.isLeader = false
	} else if rf.job == Candidate && rf.term <= args.Term {
		rf.term = args.Term
		rf.job = Follower
		rf.isLeader = false
	}

	if len(rf.logs)-1 == args.PrevLogIndex && rf.logs[len(rf.logs)-1].Term == args.PrevLogTerm {
		if len(args.Entries) == 0 { // Heartbeat
			rf.shouldReset = true
		} else { // Actual logs

		}
		reply.Success = true
		reply.Term = rf.term
		// Check if log is consistent with args
	} else {
		reply.Success = false
		reply.Term = rf.term
	}
	rf.mu.Unlock()

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	rf.mu.Lock()
	isLeader = rf.job == Leader
	term = rf.term
	index = len(rf.logs)
	DPrintf(dStart, "[S%d]", rf.me)
	// Add to log?
	rf.mu.Unlock()

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Creates an election. Some how kill previous election?
func (rf *Raft) startElection() {
	rf.mu.Lock()
	electionTerm := rf.term
	candidateId := rf.me
	cluster_size := len(rf.peers)
	DPrintf(dElect, "[S%d] started election term %d\n", candidateId, electionTerm)
	rf.mu.Unlock()

	voteCount := 1
	responses := 1

	for id := 0; id < len(rf.peers); id++ {
		if id != candidateId {
			args := RequestVoteArgs{Term: electionTerm, CandidateId: candidateId, LastLogIndex: -1, LastLogTerm: -1}
			reply := RequestVoteReply{}
			go func(peer_id int) {
				DPrintf(dElect, "[S%d] requestVote [S%d]", candidateId, peer_id)
				rf.sendRequestVote(peer_id, &args, &reply)
				rf.mu.Lock()
				DPrintf(dElect, "[S%d] requestVote response from [S%d]\n", candidateId, peer_id)
				if reply.Term > electionTerm {
					rf.term = reply.Term
					rf.job = Follower
				}
				if reply.VoteGranted {
					voteCount += 1
					DPrintf(dTimer, "[S%d] voted updated: %d\n", candidateId, voteCount)
				}
				responses += 1
				rf.mu.Unlock()
			}(id)
		} else { // Do something for itself to guard against other votes requests?

		}
	}
	// Became follower -> entered majority check -> will fail to become leader.
	// What happens to the majority of servers which voted for this candidate?
	wonElection := false
	for {
		rf.mu.Lock()
		// Check if this election has expired
		if rf.term != electionTerm { // The only way this condition is met if rf.term has been incremented which can only occur by AEs and timers
			break
		}
		// Check if we got demoted
		if rf.job == Follower { // Got demoted
			break
		}
		curVoteCount := voteCount
		if curVoteCount > cluster_size/2 { // Won election
			DPrintf(dElect, "[S%d] won election for term %d with %d/%d votes \n", candidateId, electionTerm, curVoteCount, cluster_size)
			wonElection = true
			rf.isLeader = true
			rf.job = Leader
			break
		} else if responses-curVoteCount > cluster_size/2 { // Lost election
			// Don't just demote to follower. Probably just stop this election.
			break
		} else { // Undetermined election
			// TODO: Make sure that timer expires to break out of here
		}
		rf.mu.Unlock()
	}
	rf.mu.Unlock()

	if wonElection { // Start heartbeating
		DPrintf(dBeat, "[S%d] now heartbeating term %d\n", candidateId, electionTerm)

		// What happens if server gets demoted, timer expires, turns into Candidate?
		for {
			rf.mu.Lock()
			if rf.term > electionTerm {
				// No longer leader
				break
			}
			if rf.job == Follower {
				// No longer leader
				break
			}
			rf.mu.Unlock()
			for id := 0; id < len(rf.peers); id++ {
				if id != candidateId {
					args := AppendEntriesArgs{Term: electionTerm, LeaderId: candidateId, PrevLogIndex: 0, PrevLogTerm: 0, Entries: make([]Log, 0), LeaderCommit: -1}
					reply := AppendEntriesReply{}
					go func(peer_id int) {
						rf.sendAppendEntries(peer_id, &args, &reply)
						DPrintf(dBeat, "[S%d] hearbeat response from [S%d] with success=%t\n", candidateId, peer_id, reply.Success)
						rf.mu.Lock()
						if reply.Term > electionTerm {
							rf.term = reply.Term
							rf.job = Follower
							rf.isLeader = false
						}
						rf.mu.Unlock()
					}(id)
				}
			}
			// Do I need to wait for all responses back?
			time.Sleep(200 * time.Millisecond) // Sleep for 200 ms

		}
		rf.mu.Unlock()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	timeout := rand.Intn(400) + 1000
	for rf.killed() == false {

		time.Sleep(time.Duration(timeout) * time.Millisecond)

		rf.mu.Lock()
		DPrintf(dTick, "[S%d] woke up. shouldReset=%t \n", rf.me, rf.shouldReset)
		if rf.shouldReset {
			DPrintf(dTick, "[S%d] timer reset\n", rf.me)
			timeout = rand.Intn(400) + 1000
			rf.shouldReset = false
		} else { // Timer has expired. Turn into candidate
			if rf.job == Leader {
				DPrintf(dTick, "[S%d] is leader\n", rf.me)
				rf.mu.Unlock()
				continue
			}
			if rf.job == Follower { // You become a candidate, but someone else requests your vote. Must reject atht one
				DPrintf(dTick, "[S%d] promoted to CANDIDATE\n", rf.me)
				rf.job = Candidate
				rf.isLeader = false
			}
			rf.term += 1
			timeout = rand.Intn(400) + 1000

			// Vote for yourself
			rf.votedFor = rf.me
			go rf.startElection()
		}
		rf.mu.Unlock()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.job = Follower
	rf.isLeader = false
	rf.term = 0
	// nextIndex = ?
	rf.matchIndex = make([]int, len(peers))
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.shouldReset = false
	rf.logs = make([]Log, 0)
	nil_log := Log{Term: 0}
	rf.logs = append(rf.logs, nil_log)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
