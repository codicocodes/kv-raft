package raft

// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
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

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// NOTE: For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu           sync.Mutex          // Lock to protect shared access to this peer's state
	peers        []*labrpc.ClientEnd // RPC end points of all peers
	persister    *Persister          // Object to hold this peer's persisted state
	me           int                 // this peer's index into peers[]
	dead         int32               // set by Kill()
	currentTerm  int
	votedFor     *int
	leader       *int
	lastAppliedTime time.Time
	role Role
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
	if rf.leader != nil {
		fmt.Printf("[GetState.%d.%d] The Leader: %d\n", rf.currentTerm, rf.me, *rf.leader)
	}
	term = rf.currentTerm
	isleader = rf.leader == &rf.me
	return term, isleader
}
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
) 

func (rf *Raft) getRole() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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


// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// TODO: Your data here (2A, 2B).
	CandidateId int
	Term int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf(
		"[RequestVote.%d.%d]: Received from %d\n",
		rf.currentTerm,
		rf.me,
		args.CandidateId,
	)
	// TODO: Your code here (2A, 2B).
	// ❓ votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if args.Term <= rf.currentTerm {
		// ✅. Reply false if term < currentTerm (§5.1)
		// doing <= because they might already voted for another candidate
		fmt.Printf(
			"[RequestVote.%d.%d]: Voting NO for %d because the term %d is too low\n",
			rf.currentTerm,
			rf.me,
			args.CandidateId,
			args.Term,
		)
		reply.VoteGranted = false
		return
	}

	rf.lastAppliedTime = time.Now()
	rf.currentTerm = args.Term
	rf.votedFor = &args.CandidateId
	reply.VoteGranted = true
	fmt.Printf(
		"[RequestVote.%d.%d]: Voting YES for %d\n",
		rf.currentTerm,
		rf.me,
		args.CandidateId,
	)
}

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
func (rf *Raft) sendRequestVote(
	server int, 
	args *RequestVoteArgs, 
	reply *RequestVoteReply, 
	voteCh chan int,
) bool {
	before := time.Now()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	fmt.Printf("[sendRequestVote.%d.%d] to node %d took time: %d\n", args.Term, args.CandidateId, server, time.Since(before).Milliseconds())
	if ok && reply.VoteGranted {
		voteCh <- 1
	} else {
		voteCh <- 0
	}
	return ok
}

type RequestAppendEntriesReply struct {}

type RequestAppendEntriesArgs struct {
	Term int
	Leader int
}

func (rf *Raft) sendAppendEntries(
	server int, 
	args *RequestAppendEntriesArgs, 
	reply *RequestAppendEntriesReply,
) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// NOTE: Your code here (2B).

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) AppendEntries(
	req *RequestAppendEntriesArgs, 
	reply *RequestAppendEntriesReply,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if req.Term < rf.currentTerm {
		fmt.Printf("[AppendEntries]: %d is denying append entry due to low term.\n", rf.me)
		return
	}
	rf.role = Follower
	rf.leader = &req.Leader
	rf.currentTerm = req.Term
	rf.lastAppliedTime = time.Now()
	fmt.Printf("[AppendEntries.%d.%d]: Writing Entry.\n", rf.currentTerm, rf.me)
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role == Leader
}

func (rf *Raft) sendHeartbeat()  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastAppliedTime = time.Now()
	leader := rf.me
	term := rf.currentTerm
	for i := range rf.peers {
		if i != rf.me {
			var (
				args RequestAppendEntriesArgs
				reply RequestAppendEntriesReply
			)				
			args.Leader = leader
			args.Term = term
			go rf.sendAppendEntries(i, &args, &reply)
		}
	}
}

func (rf *Raft) runLeader() {
	for !rf.killed() && rf.isLeader() {
		rf.sendHeartbeat()
		time.Sleep(time.Millisecond * time.Duration(100))
	}
}

func (rf *Raft) sendAllVoteRequests(voteCh chan int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
	rf.votedFor=&rf.me
	rf.lastAppliedTime = time.Now()
	fmt.Printf("[becomeCandidate.%d.%d] Start\n", rf.currentTerm, rf.me)
	for i := range rf.peers {
		if i != rf.me {
			var (
				args RequestVoteArgs
				reply RequestVoteReply
			)				
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			go rf.sendRequestVote(i, &args, &reply, voteCh)
		}
	}
}

func (rf *Raft) countVotes(voteCh chan int) int  {
	voteOver := make(chan bool)
	fmt.Printf("[becomeCandidate.%d.%d] Waiting\n", rf.currentTerm, rf.me)
	totalVotes := 1
	yesVote := 1 // vote for myself
	go func () {
		time.Sleep(getRandomTickerDuration())
		voteOver <- true
	}()
	for {
		select {
		case <- voteOver:
				return yesVote
		case vote := <- voteCh:
			yesVote += vote
			totalVotes++
			if yesVote > len(rf.peers) / 2 {
				// we have a majority
				return yesVote
			}
			if totalVotes - yesVote > len(rf.peers) / 2 {
				// no has a majority 😢
				return yesVote
			}
			if totalVotes == len(rf.peers) {
				// everyone voted
				return yesVote
			}
		}
	}
}


func (rf *Raft) electionOutcome(yesVotes int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if yesVotes > len(rf.peers) / 2 {
		fmt.Printf("[becomeCandidate.%d.%d] Taking leadership\n", rf.currentTerm, rf.me)
		rf.leader = &rf.me
		rf.role = Leader
		return
	}
	rf.role = Follower
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = Candidate
}

func (rf *Raft) missingHeartbeat(ms time.Duration) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastAppliedTime.Before(
		time.Now().Add(-1 * ms),
	)
}

func getRandomTickerDuration() time.Duration {
	min := 150
	max := 300
	return time.Millisecond * time.Duration(rand.Intn(max - min) + min)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		switch rf.getRole() {
		case Follower:
			rf.runFollower()
		case Candidate:
			rf.runCandidate()
		case Leader:
			rf.runLeader()
		}
	}
}

func (rf *Raft) runFollower() {
	sleepDuration := getRandomTickerDuration()
	time.Sleep(sleepDuration)
	if rf.missingHeartbeat(sleepDuration) {
		rf.becomeCandidate()
	}
}

type Election struct {
	voteCh chan int
	yesVotes int
}

func (rf *Raft) runCandidate() {
	voteCh := make(chan int)
	rf.sendAllVoteRequests(voteCh)
	yesVotes := rf.countVotes(voteCh)
	rf.electionOutcome(yesVotes)
}



// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, _ chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = nil
	rf.lastAppliedTime = time.Time{}
	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()
	return rf
}
