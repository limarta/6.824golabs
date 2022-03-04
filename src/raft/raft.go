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
	term        int
	nextIndex   []int
	matchIndex  []int // Initialized to 0
	commitIndex int
	lastApplied int
	shouldReset bool
	logs        []Log
	votedFor    int
	applyCh     chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	term = rf.term
	isleader = rf.job == Leader
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//Check if leader, follower, candidate?
	rf.mu.Lock()
	DPrintf(dReqVote, "[S%d] (term=%d) <- [S%d] (election term=%d)\n", rf.me, rf.term, args.CandidateId, args.Term)
	// TODO: Must check Complete Leader condition. Incomplete right now.
	if rf.term > args.Term { // Candidate is old. Reject.
		DPrintf(dIgnore, "[S%d] VoteReq from [S%d]", rf.me, args.CandidateId)
		reply.Term = rf.term
		reply.VoteGranted = false
	} else if rf.term < args.Term {
		DPrintf(dNewTerm, "[S%d] in RequestVote() (old term=%d) (new term=%d)", rf.me, rf.term, args.Term)
		if rf.job == Leader {
			DPrintf(dReqVote, "[S%d] valid VoteReq [S%d] from Leader", rf.me, args.CandidateId)
		} else if rf.job == Candidate {
			DPrintf(dReqVote, "[S%d] valid VoteReq [S%d] from Candidate", rf.me, args.CandidateId)
		}
		rf.term = args.Term
		rf.job = Follower
		// Election Restriction
		if args.LastLogTerm > rf.logs[len(rf.logs)-1].Term { // Candidate is more up to date
			rf.votedFor = args.CandidateId
			rf.shouldReset = true
			reply.Term = rf.term
			reply.VoteGranted = true
			DPrintf(dReqVote, "[S%d] ACCEPTED [S%d]. Last log higher term", rf.me, args.CandidateId)
		} else if args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex >= len(rf.logs)-1 { // Candidate is more up to date
			rf.votedFor = args.CandidateId
			rf.shouldReset = true
			reply.Term = rf.term
			reply.VoteGranted = true
			DPrintf(dReqVote, "[S%d] ACCEPTED [S%d]. Equal term. Higher index", rf.me, args.CandidateId)
		} else { // Voter is more up to date // Make sure candidate does not call itself
			DPrintf(dReqVote, "[S%d] REJECTED [S%d]", rf.me, args.CandidateId)
			reply.Term = rf.term
			reply.VoteGranted = false
		}
	} else {
		reply.Term = rf.term // Same term. Make sure candidate doesn't call on itself
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Make sure server has been initialized before others servers can successfully append/get votes

	DPrintf(dAppend, "[S%d] <- [S%d]", rf.me, args.LeaderId)
	rf.mu.Lock()
	if rf.term > args.Term { // Received AppendEntries from old leader
		DPrintf(dIgnore, "[S%d] AppendEntry from [S%d]", rf.me, args.LeaderId)
		reply.Success = false
		reply.Term = rf.term // For old leader to update itself
		rf.mu.Unlock()
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

	rf.shouldReset = true // Is this correct?
	reply.Term = rf.term

	if len(args.Entries) == 0 {
		DPrintf(dBeat, "[S%d] received heartbeat [S%d] (commitIndex=%d)", rf.me, args.LeaderId, rf.commitIndex)
	}
	if len(rf.logs)-1 < args.PrevLogIndex { // Missing logs
		DPrintf(dAppend, "[S%d] missing logs (follower logs=%v) (prevLogIndex=%d)", rf.me, rf.logs, args.PrevLogIndex)
		reply.Success = false
		reply.Term = rf.term
	} else if len(rf.logs)-1 > args.PrevLogIndex { // Cut out extraneous logs
		DPrintf(dAppend, "[S%d] cut logs (follower logs=%v) (prevLogIndex=%d) (prevLogTerm=%d) (entries=%v)", rf.me, rf.logs, args.PrevLogIndex, args.PrevLogTerm, args.Entries)
		if rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm {
			rf.logs = rf.logs[0 : args.PrevLogIndex+1] // Remove last entry to save time?
		} else {
			rf.logs = rf.logs[0:args.PrevLogIndex] // Remove last entry to save time?
		}
		reply.Success = false
		reply.Term = rf.term
	}
	if len(rf.logs)-1 == args.PrevLogIndex {
		if rf.logs[len(rf.logs)-1].Term == args.PrevLogTerm { // Append logs also includes empty ones
			if len(args.Entries) != 0 {
				DPrintf(dAppend, "[S%d] APPENDED (old log=%v) (new log=%v)", rf.me, rf.logs, append(rf.logs, args.Entries...))
			} else {
				DPrintf(dBeat, "[S%d] agreed with beat (log=%v)", rf.me, rf.logs)
			}
			rf.logs = append(rf.logs, args.Entries...)
			reply.Success = true
			reply.Term = rf.term
			if rf.commitIndex < args.LeaderCommit {
				newCommitIndex := min(args.LeaderCommit, len(rf.logs)-1)
				DPrintf(dAppend, "[S%d] updated (old commitIndex=%d) (new commitIndex=%d)", rf.me, rf.commitIndex, newCommitIndex)
				// Move this somewhere else?
				for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
					msg := ApplyMsg{CommandValid: true, Command: rf.logs[i].Command, CommandIndex: i}
					DPrintf(dApply, "[S%d] commitIndex=%d (log=%v)", rf.me, i, rf.logs[i])
					rf.applyCh <- msg // Send to client
				}
				rf.commitIndex = newCommitIndex
			}

		} else { // Right spot but wrong term
			DPrintf(dAppend, "[S%d] had right (index=%d) but different (term=%d) vs (entry_term=%d)", rf.me, args.PrevLogIndex, rf.logs[len(rf.logs)-1].Term, args.PrevLogTerm)
		}
	}
	rf.mu.Unlock()
}

func min(x int, y int) int {
	if x <= y {
		return x
	} else {
		return y
	}
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
	isLeader = rf.job == Leader
	if isLeader {
		term = rf.term
		index = len(rf.logs)
		newLog := Log{Term: term, Command: command}
		rf.logs = append(rf.logs, newLog)
		rf.nextIndex[rf.me] = len(rf.logs)
		DPrintf(dStart, "[S%d] (cmd=%v) (isLeader=%t) (index=%d)", rf.me, command, isLeader, index)
	}
	// Go routine here to send stuff out?
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

func (rf *Raft) forwardCommits(electionTerm int) {
	for rf.killed() == false {
		time.Sleep(100 * time.Millisecond)

		rf.mu.Lock()
		if rf.job != Leader || rf.term != electionTerm {
			rf.mu.Unlock()
			break
		}
		rf.matchIndex[rf.me] = len(rf.logs) - 1
		DPrintf(dCommit, "[S%d] (isLeader=%t) (currentCommit=%d) (matchIndex=%v)", rf.me, rf.job == Leader, rf.commitIndex, rf.matchIndex)
		// Try to verify one more commit and send to client
		if rf.commitIndex < len(rf.logs)-1 {
			N := rf.commitIndex + 1
			for ; N < len(rf.logs); N++ {
				if rf.logs[N].Term == rf.term {
					break
				}
			} // Do I have to try future N?
			if N == len(rf.logs) { // Could not find an N
				DPrintf(dCommit, "[S%d] could not find an N: (term=%d) (log=%v)", rf.me, rf.term, rf.logs)
				rf.mu.Unlock()
				continue
			}
			DPrintf(dCommit, "[S%d] first (N=%d) of (term=%d) (log=%v)", rf.me, N, rf.term, rf.logs)
			tally := 0
			for id := 0; id < len(rf.peers); id++ {
				if rf.matchIndex[id] >= N {
					tally += 1
				}
			}
			if tally > len(rf.peers)/2 {
				for i := rf.commitIndex + 1; i <= N; i++ {
					DPrintf(dCommit, "[S%d] (NEW commit=%d)", rf.me, i)
					msg := ApplyMsg{CommandValid: true, Command: rf.logs[i].Command, CommandIndex: i}
					DPrintf(dApply, "[S%d] (commitIndex=%d) (log=%v)", rf.me, i, rf.logs[i])
					rf.applyCh <- msg // Send to client
				}
				rf.commitIndex = N
			} else {
				DPrintf(dCommit, "[S%d] invalid N (tally=%d/%d)", rf.me, tally, len(rf.peers))
			}
		}
		// 	} else if rf.job == Follower {
		rf.mu.Unlock()

	}
}

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
					nextIndex := rf.nextIndex[peer_id]
					logSize := len(rf.logs)
					if logSize > nextIndex {
						prevLogIndex := rf.nextIndex[peer_id] - 1
						prevLogTerm := rf.logs[prevLogIndex].Term
						leaderCommit := rf.commitIndex
						entries := rf.logs[rf.nextIndex[peer_id]:]
						args := AppendEntriesArgs{Term: rf.term, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm,
							Entries: entries, LeaderCommit: leaderCommit}
						reply := AppendEntriesReply{}
						DPrintf(dAppendListen, "[S%d] -> [S%d] new (entries=%v)", rf.me, peer_id, entries)
						rf.mu.Unlock()

						rf.sendAppendEntries(peer_id, &args, &reply)
						rf.mu.Lock()
						// Leader may have changed between these steps?
						if rf.term == electionTerm && rf.term == reply.Term {
							if reply.Success {
								rf.nextIndex[peer_id] = logSize
								rf.matchIndex[peer_id] = logSize - 1
								DPrintf(dAppendListen, "[S%d] successful replication in [S%d]", rf.me, peer_id)
								DPrintf(dAppendListen, "[S%d] (nextIndex[S%d]=%v) (matchIndex[S%d]=%v)", rf.me, peer_id, rf.nextIndex, peer_id, rf.matchIndex)
							} else if reply.Term != -1 {
								DPrintf(dDecreaseIndex, "[S%d] decrement nextIndex for [S%d]", rf.me, peer_id)
								if rf.nextIndex[peer_id] > 1 {
									rf.nextIndex[peer_id]--
								}
							}
						} else if reply.Term > rf.term { // Follower follows new leader
							DPrintf(dDemote, "[S%d] in listenAppendEntries from [S%d]. (newTerm = %d)", rf.me, peer_id, reply.Term)
							rf.term = reply.Term
							rf.job = Follower
						}
						rf.mu.Unlock()
					} else {
						rf.mu.Unlock()
					}
				}
			}(id)
		}

	}
}

func (rf *Raft) heartBeat(electionTerm int) {
	// What happens if server gets demoted, timer expires, turns into Candidate?
	DPrintf(dBeat, "[S%d] initiate beating (term=%d)", rf.me, electionTerm)
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
				rf.mu.Lock()
				prevLogIndex := rf.nextIndex[id] - 1
				prevLogTerm := rf.logs[rf.nextIndex[id]-1].Term
				leaderCommit := rf.commitIndex

				args := AppendEntriesArgs{Term: electionTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: make([]Log, 0), LeaderCommit: leaderCommit}
				reply := AppendEntriesReply{}
				rf.mu.Unlock()
				go func(peer_id int) {
					DPrintf(dBeat, "[S%d] -> [S%d] (term=%d)", rf.me, peer_id, electionTerm)
					rf.sendAppendEntries(peer_id, &args, &reply)

					rf.mu.Lock()
					if rf.term == electionTerm && electionTerm == reply.Term {
						if reply.Success {
							DPrintf(dBeat, "[S%d] -> [S%d] response success (reply.term=%d)\n", rf.me, peer_id, reply.Term)
							rf.matchIndex[peer_id] = prevLogIndex
							// Set matchIndex here?
						} else if reply.Term != -1 {
							DPrintf(dDecreaseIndex, "[S%d] for [S%d]", rf.me, peer_id)
							DPrintf(dBeat, "[S%d] decrement nextIndex for [S%d] (oldIndex=%d)", rf.me, peer_id, rf.nextIndex[peer_id])
							if rf.nextIndex[peer_id] > 1 {
								rf.nextIndex[peer_id]--
							}
						}
					} else if electionTerm < reply.Term { // Follower follows new leader
						DPrintf(dDemote, "[S%d] in heartBeat() by [S%d]. (old term=%d)->(newTerm = %d)", rf.me, peer_id, electionTerm, reply.Term)
						rf.term = reply.Term
						rf.job = Follower
					}
					rf.mu.Unlock()
				}(id)
			} else { // When leader

			}
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Creates an election. Some how kill previous election?
func (rf *Raft) startElection(electionTerm int) {
	rf.mu.Lock()
	if rf.term != electionTerm {
		rf.mu.Unlock()
		return
	}
	candidateId := rf.me
	cluster_size := len(rf.peers)
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[len(rf.logs)-1].Term
	DPrintf(dElect, "[S%d] hosts (election term %d) (lastLogIndex=%d) (lastLogTerm=%d)", candidateId, electionTerm, lastLogIndex, lastLogTerm)
	DPrintf(dElect, "[S%d] candidate log: %v", rf.me, rf.logs)
	rf.mu.Unlock()

	voteCount := 1
	responses := 1

	for id := 0; id < len(rf.peers); id++ {
		if id != candidateId {
			args := RequestVoteArgs{Term: electionTerm, CandidateId: candidateId, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
			reply := RequestVoteReply{}
			go func(peer_id int) {
				DPrintf(dElect, "[S%d] -> [S%d] voteReq", candidateId, peer_id)
				rf.sendRequestVote(peer_id, &args, &reply)
				rf.mu.Lock()
				DPrintf(dElect, "[S%d] <- [S%d] voteResponse", candidateId, peer_id)
				if reply.Term > electionTerm {
					DPrintf(dDemote, "[S%d] in startElection polling", candidateId)
					rf.term = reply.Term
					rf.job = Follower
				}
				if reply.VoteGranted {
					voteCount += 1
					DPrintf(dElect, "[S%d] voted updated: %d\n", candidateId, voteCount)
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
		// Check if this election has expired or we got demoted
		if rf.term != electionTerm || rf.job == Follower { // The only way this condition is met if rf.term has been incremented which can only occur by AEs and timers
			break
		}
		curVoteCount := voteCount
		if curVoteCount > cluster_size/2 { // Won election
			DPrintf(dWon, "[S%d] won election for term %d with %d/%d votes \n", candidateId, electionTerm, curVoteCount, cluster_size)
			wonElection = true
			rf.job = Leader
			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.logs)
			}
			for i := range rf.matchIndex {
				rf.matchIndex[i] = 0
			}
			break
		} else if responses-curVoteCount > cluster_size/2 { // Lost election
			DPrintf(dLoss, "[S%d] lost for term %d with %d/%d votes", candidateId, electionTerm, responses-curVoteCount, cluster_size)
			// Don't just demote to follower. Probably just stop this election.
			break
		} else { // Undetermined election
			// TODO: Make sure that timer expires to break out of here
		}
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
	timeRange := 300
	baseTime := 700
	timeout := rand.Intn(timeRange) + baseTime
	for rf.killed() == false {

		time.Sleep(time.Duration(timeout) * time.Millisecond)

		rf.mu.Lock()
		DPrintf(dTick, "[S%d] woke up", rf.me)
		DPrintf(dLogs, "[S%d]: %v", rf.me, rf.logs)
		DPrintf(dCommit2, "[S%d]: %v", rf.me, rf.commitIndex)
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
			}
			if rf.job == Follower { // You become a candidate, but someone else requests your vote. Must reject atht one
				DPrintf(dTick, "[S%d] promoted to CANDIDATE (election_term=%d)\n", rf.me, rf.term+1)
				rf.job = Candidate
			}
			rf.term += 1
			timeout = rand.Intn(timeRange) + baseTime

			// Vote for yourself
			rf.votedFor = rf.me
			go rf.startElection(rf.term)
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
	DPrintf(dInit, "[S%d]", rf.me)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
