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
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

type Log struct {
	Term    int
	Command interface{}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	dead            int32               // set by Kill()
	job             Job
	term            int
	nextIndex       []int
	matchIndex      []int // Initialized to 0
	commitIndex     int
	shouldReset     bool
	lastApplied     int
	logs            []Log
	votedFor        int
	applyCh         chan ApplyMsg
	lastIndexChange []time.Time

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.term
	isleader = rf.job == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	DPrintf(dPersist, "[S%d]", rf.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	DPrintf(dRead, "[S%d] rebooting", rf.me)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintf(dRead, "[S%d] no backup", rf.me)
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var commitIndex int
	var logs []Log
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&logs) != nil {
		DPrintf(dRead, "[S%d] error decoding", rf.me)
		// Error?
	} else {
		rf.mu.Lock()
		rf.term = term
		rf.votedFor = votedFor
		rf.commitIndex = commitIndex
		rf.logs = logs
		DPrintf(dRead, "[S%d] restored (term=%d) (votedFor=%d) (logs=%v)", rf.me, term, votedFor, logs)
		rf.mu.Unlock()
	}
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//Check if leader, follower, candidate?
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.term > args.Term { // Candidate is old. Reject.
		DPrintf(dIgnore, "[S%d] <- [S%d] VoteReq", rf.me, args.CandidateId)
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	} else if rf.term < args.Term {
		DPrintf(dReqVote, "[S%d] <- [S%d] (old term=%d) (new term=%d) (old job=%d)", rf.me, args.CandidateId, rf.term, args.Term, rf.job)
		rf.term = args.Term
		rf.job = Follower
		// Election Restriction
		if (args.LastLogTerm > rf.logs[len(rf.logs)-1].Term) ||
			(args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex >= len(rf.logs)-1) { // Candidate is more up to date
			DPrintf(dReqVote, "[S%d] <- [S%d] ACCEPTED", rf.me, args.CandidateId)
			rf.votedFor = args.CandidateId
			rf.shouldReset = true
			reply.Term = rf.term
			reply.VoteGranted = true
		} else { // Voter is more up to date // Make sure candidate does not call itself
			DPrintf(dReqVote, "[S%d] <- [S%d] REJECTED", rf.me, args.CandidateId)
			reply.Term = rf.term
			reply.VoteGranted = false
		}
		// rf.persist() // What happens if it votes then dies?
	} else { // rf.term == args.Term
		reply.Term = rf.term // Same term. Make sure candidate doesn't call on itself
		reply.VoteGranted = false
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Make sure server has been initialized before others servers can successfully append/get votes

	DPrintf(dAppend, "[S%d] <- [S%d]", rf.me, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.term > args.Term { // Received AppendEntries from old leader
		DPrintf(dIgnore, "[S%d] AppendEntry from [S%d]", rf.me, args.LeaderId)
		reply.Success = false
		reply.Term = rf.term // For old leader to update itself
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		return
	}
	if rf.term < args.Term { // Server is outdated. Note: What about rf.term == args.term?
		if rf.job == Leader {
			DPrintf(dDemote, "[S%d] in AppendEntries() from Leader", rf.me)
		} else if rf.job == Candidate {
			DPrintf(dDemote, "[S%d] in AppendEntries() from Candidate", rf.me)
		}
		DPrintf(dNewTerm, "[S%d] in AppendEntries() (old term=%d) (new term=%d)", rf.me, rf.term, args.Term)
		rf.term = args.Term
		rf.job = Follower
	} else if rf.job == Candidate && rf.term == args.Term {
		DPrintf(dDemote, "[S%d] in AppendEntries() from Candidate", rf.me)
		rf.term = args.Term
		rf.job = Follower
	}

	rf.shouldReset = true
	reply.Term = rf.term
	DPrintf(dAppend, "[S%d] (log=%v) (entry=%v)", rf.me, rf.logs, args.Entries)
	if len(rf.logs)-1 < args.PrevLogIndex { // Log too short
		reply.Success = false
		reply.ConflictIndex = len(rf.logs)
		reply.ConflictTerm = -1
		DPrintf(dAppend, "[S%d] (isBeat=%t) missing logs (follower logs=%v) (log len=%d) (prevLogIndex=%d) (prevLogTerm=%d) (conflictIndex=%d) (conflictTerm=%d)",
			rf.me, len(args.Entries) == 0, rf.logs, len(rf.logs), args.PrevLogIndex, args.PrevLogTerm, reply.ConflictIndex, reply.ConflictTerm)
		// rf.persist()
		return
	} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm { // Has index; wrong term
		DPrintf(dAppend, "[S%d] (isBeat=%t) right (prevLogIndex=%d) wrong (log prevLogTerm=%d) (entry prevLogTerm=%d)",
			rf.me, len(args.Entries) == 0, args.PrevLogIndex, rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm)
		reply.Success = false
		reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
		i := 0
		for i <= args.PrevLogIndex {
			if rf.logs[i].Term == reply.ConflictTerm {
				break
			}
			i++
		}
		reply.ConflictIndex = i
		// rf.persist()
		return
	}

	reply.Success = true
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		panic("FAILED term assumption")
	}
	DPrintf(dAppend, "[S%d] <- [S%d] AGREED (isBeat=%t) (leader prevLogIndex=%d) (follower prevLogTerm=%d) (leader prevLogTerm=%d) (len log=%d) (len entries=%d)",
		rf.me, args.LeaderId, len(args.Entries) == 0, args.PrevLogIndex, rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm, len(rf.logs), len(args.Entries))
	i := 0
	for ; i < len(args.Entries) && args.PrevLogIndex+i+1 < len(rf.logs); i++ {
		if rf.logs[i+args.PrevLogIndex+1] != args.Entries[i] {
			break
		}
	}

	if i < len(args.Entries) { // All entries match follower's log
		DPrintf(dAppend, "[S%d] didn't match at (i=%d)", rf.me, i)
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
		DPrintf(dAppend, "[S%d] (new log=%v)", rf.me, rf.logs)
	} else {
		DPrintf(dAppend, "[S%d] all entries match follower's log", rf.me)
	}

	reply.Success = true
	DPrintf(dAppend, "[S%d] <- [S%d] (commitIndex=%d) (leader commitIndex=%d)", rf.me, args.LeaderId, rf.commitIndex, args.LeaderCommit)
	newCommitIndex := rf.commitIndex
	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		DPrintf(dAppend, "[S%d] updated (old commitIndex=%d) (new commitIndex=%d)", rf.me, rf.commitIndex, newCommitIndex)
	}
	for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
		msg := ApplyMsg{CommandValid: true, Command: rf.logs[i].Command, CommandIndex: i}
		DPrintf(dApply, "[S%d] commitIndex=%d (log=%v)", rf.me, i, rf.logs[i])
		rf.applyCh <- msg // Send to client
	}
	rf.commitIndex = newCommitIndex

	// rf.persist()
}

func min(x int, y int) int {
	if x <= y {
		return x
	} else {
		return y
	}
}

func max(x int, y int) int {
	if x < y {
		return y
	}
	return x
}

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
	defer rf.mu.Unlock()
	isLeader = rf.job == Leader
	if isLeader {
		term = rf.term
		index = len(rf.logs)
		newLog := Log{Term: term, Command: command}
		rf.logs = append(rf.logs, newLog)
		rf.nextIndex[rf.me] = len(rf.logs)
		DPrintf(dStart, "[S%d] (cmd=%v) (isLeader=%t) (index=%d)", rf.me, command, isLeader, index)
		// rf.persist()
	}

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

//
// the leader must determine which logs have been commited. only logs that have been appended
// during the leader's term can be commited. previous logs cannot but the Log Matching property
// guarantees that such logs may be committed.
func (rf *Raft) forwardCommits(electionTerm int) {
	for rf.killed() == false {
		time.Sleep(50 * time.Millisecond)

		rf.mu.Lock()
		if rf.job != Leader || rf.term != electionTerm {
			rf.mu.Unlock()
			break
		}
		rf.matchIndex[rf.me] = len(rf.logs) - 1
		DPrintf(dCommit, "[S%d] (term=%d) (currentCommit=%d) (matchIndex=%v) (nextIndex=%v)",
			rf.me, rf.term, rf.commitIndex, rf.matchIndex, rf.nextIndex)

		if rf.commitIndex < len(rf.logs)-1 {
			matchCopy := make([]int, len(rf.matchIndex))
			copy(matchCopy, rf.matchIndex)

			N := getMedian(matchCopy)
			for ; N > rf.commitIndex; N-- {
				if rf.logs[N].Term == rf.term {
					break
				}
			}

			if N == rf.commitIndex { // Could not find an N
				DPrintf(dCommit, "[S%d] no new N (term=%d) (log=%d)", rf.me, rf.term, rf.logs)
			} else {
				DPrintf(dCommit, "[S%d] FOUND (N=%d) (term=%d) (log=%v)", rf.me, N, rf.term, rf.logs)
				for i := rf.commitIndex + 1; i <= N; i++ {
					msg := ApplyMsg{CommandValid: true, Command: rf.logs[i].Command, CommandIndex: i}
					DPrintf(dApply, "[S%d] (commitIndex=%d) (log=%v)", rf.me, i, rf.logs[i])
					rf.applyCh <- msg // Send to client
				}
				rf.commitIndex = N

			}
			// rf.persist()
		}
		rf.mu.Unlock()
	}
}

func getMedian(arr []int) int {
	sort.Ints(arr)
	if len(arr)%2 == 0 {
		return arr[len(arr)/2-1]
	} else {
		return arr[len(arr)/2]
	}
}

// the leader continuously attempts to send logs to its followers. updates the nextIndex and matchIndex for
// each follower except its own
func (rf *Raft) sendAppendEntriesToAll(electionTerm int) {
	for id := 0; id < len(rf.peers); id++ {
		if id != rf.me {
			go func(peer_id int) {
				DPrintf(dAppendListen, "[S%d] ready for [S%d]", rf.me, peer_id)
				for rf.killed() == false {
					time.Sleep(100 * time.Millisecond) // Suggested time buffering from lab
					rf.mu.Lock()
					if rf.job != Leader || rf.term != electionTerm {
						rf.mu.Unlock()
						break
					}
					rf.mu.Unlock()
					rf.sendAppendEntryToPeer(peer_id, electionTerm, false)
				}
			}(id)
		}
	}
}

func (rf *Raft) sendAppendEntryToPeer(peer_id int, electionTerm int, beat bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.job != Leader || rf.term != electionTerm {
		return
	}

	nextIndex := rf.nextIndex[peer_id]
	matchIndex := rf.matchIndex[peer_id]
	args := AppendEntriesArgs{
		Term:         rf.term,
		LeaderId:     rf.me,
		PrevLogIndex: nextIndex - 1,
		PrevLogTerm:  rf.logs[nextIndex-1].Term,
		LeaderCommit: rf.commitIndex}
	reply := AppendEntriesReply{}
	if beat {
		args.Entries = make([]Log, 0)
		DPrintf(dBeat, "[S%d] -> [S%d] (term=%d) (prevLogIndex=%d) (prevLogTerm=%d) (leaderCommit=%d) (matchIndex=%v) (nextIndex=%v) (entries=%v)",
			rf.me, peer_id, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, rf.matchIndex, rf.nextIndex, args.Entries)
	} else if len(rf.logs)-1 >= nextIndex {
		args.Entries = rf.logs[nextIndex:]
		DPrintf(dAppendListen, "[S%d] -> [S%d] (term=%d) (prevLogIndex=%d) (prevLogTerm=%d) (leaderCommit=%d) (matchIndex=%v) (nextIndex=%v) (entries=%v)",
			rf.me, peer_id, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, rf.matchIndex, rf.nextIndex, args.Entries)
	} else {
		DPrintf(dAppendListen, "[S%d] nothing to send to [S%d] (len logs=%d) (nextIndex=%d)", rf.me, peer_id, len(rf.logs), nextIndex)
		return
	}

	rf.mu.Unlock()

	rf.sendAppendEntries(peer_id, &args, &reply)

	rf.mu.Lock()
	if rf.term == electionTerm && rf.term == reply.Term { // Leader may have changed between these steps?
		if reply.Success {
			if beat {
				DPrintf(dBeat, "[S%d] -> [S%d] SUCCESS. (old matchIndex=%v) (old nextIndex=%v)", rf.me, peer_id, rf.matchIndex, rf.nextIndex)
			} else {
				DPrintf(dAppendListen, "[S%d] -> [S%d] SUCCESS. (old matchIndex=%v) (old nextIndex=%v)",
					rf.me, peer_id, rf.matchIndex, rf.nextIndex)
			}
			rf.matchIndex[peer_id] = max(rf.matchIndex[peer_id], args.PrevLogIndex+len(args.Entries)) // Follower logs could have changed?
			rf.nextIndex[peer_id] = rf.matchIndex[peer_id] + 1
			if beat {
				DPrintf(dBeat, "[S%d] -> [S%d] updated (new matchIndex=%v) (new nextIndex=%v)",
					rf.me, peer_id, rf.matchIndex, rf.nextIndex)
			} else {
				DPrintf(dAppendListen, "[S%d] -> [S%d] updated (new matchIndex=%v) (new nextIndex=%v)",
					rf.me, peer_id, rf.matchIndex, rf.nextIndex)
			}
		} else {
			if reply.Term > rf.term {
				panic("reply.Term < rf.term")
			}
			if beat {
				DPrintf(dBeat, "[S%d] -> [S%d] FAILED (current nextIndex=%v)", rf.me, peer_id, rf.nextIndex)
			} else {
				DPrintf(dAppendListen, "[S%d] -> [S%d] FAILED (current nextIndex=%v)", rf.me, peer_id, rf.nextIndex)
			}
			if rf.matchIndex[peer_id] == matchIndex {
				DPrintf(dDecreaseIndex, "[S%d] -> [S%d] (saved nextIndex=%d) (nextIndex=%v) (matchIndex=%v)",
					rf.me, peer_id, nextIndex, rf.nextIndex, rf.matchIndex)
				if reply.ConflictTerm == -1 {
					rf.nextIndex[peer_id] = max(1, min(reply.ConflictIndex, rf.nextIndex[peer_id]))
					DPrintf(dConflict, "[S%d] -> [S%d] (reply.ConflictIndex=%d) (saved nextIndex=%d) (new nextIndex=%v) (matchIndex=%v)",
						rf.me, peer_id, reply.ConflictIndex, nextIndex, rf.nextIndex, rf.matchIndex)
				} else {
					found := false
					i := len(rf.logs) - 1
					for ; i >= 0; i-- {
						if rf.logs[i].Term == reply.ConflictTerm {
							found = true
							break
						}
					}
					if i > args.PrevLogIndex {
						panic("i>args.PrevLogIndex")
					}
					if found {
						DPrintf(dConflict, "[S%d] -> [S%d] found (conflictTerm=%d) (i=%d)", rf.me, peer_id, reply.ConflictTerm, i)
						rf.nextIndex[peer_id] = max(1, min(i, rf.nextIndex[peer_id]))
					} else {
						DPrintf(dConflict, "[S%d] -> [S%d] NOT found (conflictTerm=%d)", rf.me, peer_id, reply.ConflictTerm)
						rf.nextIndex[peer_id] = max(1, min(reply.ConflictIndex, rf.nextIndex[peer_id]))
					}
					DPrintf(dConflict, "[S%d] -> [S%d] (reply.ConflictIndex=%d) (reply.ConflictTerm=%d) (saved nextIndex=%d) (new nextIndex=%v) (matchIndex=%v)",
						rf.me, peer_id, reply.ConflictIndex, reply.ConflictTerm, nextIndex, rf.nextIndex, rf.matchIndex)
				}
			} else if rf.matchIndex[peer_id] > matchIndex {
				if beat {
					DPrintf(dBeat, "[S%d] -> [S%d] skip updates (rf.matchIndex=%v) (nextIndex=%v) (matchIndex=%d)",
						rf.me, peer_id, rf.matchIndex, rf.nextIndex, matchIndex)
				} else {
					DPrintf(dAppendListen, "[S%d] -> [S%d] skip updates (rf.matchIndex=%v) (nextIndex=%v) (matchIndex=%d)",
						rf.me, peer_id, rf.matchIndex, rf.nextIndex, matchIndex)
				}

			} else {
				err := fmt.Sprintf("[S%d] match index monotonicity violated (rf.matchIndex[%d]=%d) (matchIndex=%d)",
					rf.me, peer_id, rf.matchIndex[peer_id], matchIndex)
				panic(err)
			}
		}
	} else if reply.Term > rf.term { // Follower follows new leader
		DPrintf(dDemote, "[S%d] -> [S%d] (beat=%t) (oldTerm = %d) (newTerm = %d)", rf.me, peer_id, beat, rf.term, reply.Term)
		rf.term = reply.Term
		rf.job = Follower
		// rf.persist()
	}
}

func (rf *Raft) heartBeat(electionTerm int) {
	// What happens if server gets demoted, timer expires, turns into Candidate?
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.job != Leader || rf.term != electionTerm {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		DPrintf(dBeat, "[S%d] beating (term=%d)", rf.me, electionTerm)
		for id := 0; id < len(rf.peers); id++ {
			if id != rf.me {
				go func(peer_id int) {
					rf.sendAppendEntryToPeer(peer_id, electionTerm, true)
				}(id)
			}
		}
		time.Sleep(101 * time.Millisecond)
	}
}

// Creates an election. Some how kill previous election?
func (rf *Raft) startElection(electionTerm int) {
	rf.mu.Lock()
	if rf.term != electionTerm {
		rf.mu.Unlock()
		return
	}
	cluster_size := len(rf.peers)
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[len(rf.logs)-1].Term
	DPrintf(dElect, "[S%d] hosts (election term %d) (lastLogIndex=%d) (lastLogTerm=%d)", rf.me, electionTerm, lastLogIndex, lastLogTerm)
	DPrintf(dElect, "[S%d] candidate log: %v", rf.me, rf.logs)
	rf.mu.Unlock()

	voteCount := 1
	responses := 1

	for id := 0; id < len(rf.peers); id++ {
		if id != rf.me {
			args := RequestVoteArgs{Term: electionTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
			reply := RequestVoteReply{}
			go func(peer_id int) {
				DPrintf(dElect, "[S%d] -> [S%d] voteReq", rf.me, peer_id)
				rf.sendRequestVote(peer_id, &args, &reply)
				rf.mu.Lock()
				DPrintf(dElect, "[S%d] <- [S%d] voteResponse", rf.me, peer_id)
				if reply.Term > electionTerm {
					DPrintf(dDemote, "[S%d] in startElection polling", rf.me)
					rf.term = reply.Term
					rf.job = Follower
					// rf.persist()
				}
				if reply.VoteGranted {
					voteCount += 1
					DPrintf(dElect, "[S%d] voted updated: %d\n", rf.me, voteCount)
				}
				responses += 1
				rf.mu.Unlock()
			}(id)
		}
	}

	wonElection := false
	for {
		rf.mu.Lock()
		// Check if this election has expired or we got demoted
		if rf.term != electionTerm || rf.job == Follower { // The only way this condition is met if rf.term has been incremented which can only occur by AEs and timers
			break
		}
		curVoteCount := voteCount
		if curVoteCount > cluster_size/2 { // Won election
			DPrintf(dWon, "[S%d] won election for term %d with %d/%d votes \n", rf.me, electionTerm, curVoteCount, cluster_size)
			wonElection = true
			rf.job = Leader
			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.logs)
				rf.lastIndexChange[i] = time.Time{}
				rf.matchIndex[i] = 0
			}
			break
		} else if responses-curVoteCount > cluster_size/2 { // Lost election
			DPrintf(dLoss, "[S%d] lost for term %d with %d/%d votes", rf.me, electionTerm, responses-curVoteCount, cluster_size)
			// Don't just demote to follower. Probably just stop this election.
			break
		} else { // Undetermined election
			// TODO: Make sure that timer expires to break out of here
		}
		time.Sleep(2 * time.Millisecond)
		rf.mu.Unlock()
	}
	rf.mu.Unlock()

	if wonElection { // Start heartbeating
		go rf.heartBeat(electionTerm)
		go rf.sendAppendEntriesToAll(electionTerm)
		go rf.forwardCommits(electionTerm)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	timeRange := 250
	baseTime := 250
	timeout := rand.Intn(timeRange) + baseTime
	for rf.killed() == false {

		time.Sleep(time.Duration(timeout) * time.Millisecond)

		rf.mu.Lock()
		DPrintf(dTick, "[S%d] woke up", rf.me)
		if rf.shouldReset {
			DPrintf(dTick, "[S%d] resetting timer\n", rf.me)
			timeout = rand.Intn(timeRange) + baseTime
			rf.shouldReset = false
		} else { // Timer has expired. Turn into candidate
			DPrintf(dTick, "[S%d] timer expired\n", rf.me)
			if rf.job == Leader {
				DPrintf(dLeader, "[S%d] is leader (term=%d)\n", rf.me, rf.term)
				rf.mu.Unlock()
				continue
			} else if rf.job == Follower { // You become a candidate, but someone else requests your vote. Must reject atht one
				DPrintf(dTick, "[S%d] promoted to CANDIDATE (election_term=%d)\n", rf.me, rf.term+1)
				rf.job = Candidate
			}

			// Candidate votes for itself
			rf.term += 1
			rf.votedFor = rf.me
			// rf.persist()
			timeout = rand.Intn(timeRange) + baseTime
			go rf.startElection(rf.term)
		}
		rf.mu.Unlock()
	}

	close(rf.applyCh)
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
	rf.term = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.shouldReset = false
	rf.logs = make([]Log, 0)
	nil_log := Log{Term: 0}
	rf.logs = append(rf.logs, nil_log)
	rf.applyCh = applyCh
	rf.votedFor = -1
	rf.lastIndexChange = make([]time.Time, len(peers))
	// rf.persist()
	DPrintf(dInit, "[S%d]", rf.me)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	DPrintf(dRead, "[S%d] raft data: %v", rf.me, persister.raftstate)
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
