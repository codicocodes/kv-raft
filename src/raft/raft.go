package raft

// CommitHash: Refactor PrevLogIndex to PrevCommandID: 53c8fed
// Attempt at SendSnapshotRPC af800a40cd7a0f3526dd438c6e8b067b42e336a9

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

type ApplyMsg struct {
	Term         int
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// NOTE: For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

func (e LogEntry) toApplyMsg() ApplyMsg {
	return ApplyMsg{
		CommandValid: true,
		CommandIndex: e.Index,
		Term:         e.Term,
		Command:      e.Command,
	}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu               sync.Mutex          // Lock to protect shared access to this peer's state
	peers            []*labrpc.ClientEnd // RPC end points of all peers
	persister        *Persister          // Object to hold this peer's persisted state
	me               int                 // this peer's index into peers[]
	dead             int32               // set by Kill()
	currentTerm      int
	votedFor         *int
	leader           *int
	lastAppliedTime  time.Time
	lastAppliedIndex int
	role             Role
	applyCh          chan ApplyMsg
	log              []LogEntry
	commitIndex      int
	commitTerm       int
	nextIndex        []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex       []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	electionCh       chan ElectionResult
	yesVotes         int
	totalVotes       int
}

func (rf *Raft) getLastLogEntry() LogEntry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.role == Leader
	return rf.currentTerm, isLeader
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"

	default:
		panic("unexpected role")
	}
}

func (rf *Raft) getRole() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.getRaftState())
}

func (rf *Raft) getRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	if rf.votedFor != nil {
		e.Encode(rf.votedFor)
	}
	e.Encode(rf.currentTerm)
	return w.Bytes()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { 
		// bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []LogEntry
	var votedFor *int
	var currentTerm int
	if e := d.Decode(&log); e != nil {
		DPrintf(e.Error())
		panic("Fail decode log")
	}
	if e := d.Decode(&votedFor); e != nil {
		DPrintf(e.Error())
		panic("Fail decode votedFor")
	}
	if e := d.Decode(&currentTerm); e != nil {
		DPrintf("Fail decode currentTerm err=%s currentTerm=%d", e.Error(), rf.currentTerm)
		currentTerm = 0
	}
	rf.log = log
	rf.votedFor = votedFor
	rf.currentTerm = currentTerm
	rf.role = Follower
	rf.lastAppliedIndex = rf.log[0].Index
	rf.commitIndex = rf.log[0].Index
}

type RequestVoteArgs struct {
	CandidateId          int
	CandidateCommitIndex int
	Term                 int
	PrevLogIndex         int
	PrevLogTerm          int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) grantVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("[grantVote.%d.%d] to %d", rf.currentTerm, rf.me, args.CandidateId)
	rf.lastAppliedTime = time.Now()
	rf.currentTerm = args.Term
	rf.role = Follower
	rf.votedFor = &args.CandidateId
	reply.VoteGranted = true
	rf.persist()
}

func (rf *Raft) checkAlreadyVotedInTerm(args *RequestVoteArgs) bool {
	return args.Term == rf.currentTerm && rf.votedFor != nil
}

func (rf *Raft) checkIsLowTerm(args *RequestVoteArgs) bool {
	return args.Term < rf.currentTerm
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm (§5.1)
	// handle candidate is in lower term
	if rf.checkIsLowTerm(args) {
		// candidate is in a lower term
		DPrintf("Voting no due to low candiate term %d, my term %d", args.Term, rf.currentTerm)
		// VOTE NO
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		// step down if the term is higher
		rf.stepDown(args.Term)
	}

	// handle candidate is in same term
	if rf.checkAlreadyVotedInTerm(args) {
		// candidate is in same term as me but I already voted
		DPrintf("Voting no because me=%d already voted in term %d, my term %d", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	lastLog := rf.getLastLogEntry()

	// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	if lastLog.Term > args.PrevLogTerm {
		// candidates last log term is lower, voting no
		DPrintf("%d Voting no because my last entry has higher term %d, candidate PrevLogTerm %d", rf.me, lastLog.Term, args.PrevLogTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// If the logs end with the same term
	// whichever log is longer is more up-to-date
	if lastLog.Term == args.PrevLogTerm && lastLog.Index > args.PrevLogIndex {
		DPrintf("%d Voting no because my log lastLogIndex is larger %d, candidate PrevLogIndex %d", rf.me, lastLog.Index, args.PrevLogIndex)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.grantVote(args, reply)
}


type ElectionResult int

const (
	LostElection ElectionResult = iota
	WonElection
)

func (rf *Raft) wonElection() bool {
	return rf.yesVotes > len(rf.peers)/2
}

func (rf *Raft) lostElection() bool {
	return (rf.totalVotes - rf.yesVotes) > len(rf.peers)/2
}

func (rf *Raft) sendRequestVote(
	server int,
	args *RequestVoteArgs,
 	reply *RequestVoteReply,
) {
	if ok := rf.getServerByID(server).Call("Raft.RequestVote", args, reply); !ok {
		// Invalid Request: Not able to reach server. Equivalent to NO vote.
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != args.Term {
		// Invalid Request: Not in the same term
		return
	}

	if rf.role != Candidate {
		// Invalid Request: No longer candidate
		return
	}

	if reply.Term > rf.currentTerm {
		// The server responded with a higher term. I am stepping down from candidate role.
		rf.stepDown(reply.Term)
		return
	}

	rf.totalVotes++

	if reply.VoteGranted {
		rf.yesVotes++
	}

	if rf.wonElection() {
		rf.initLeaderState()
		rf.electionCh <- WonElection
		return
	}

	if rf.lostElection() {
		rf.role = Follower
		rf.electionCh <- LostElection
		return
	}
}

type RequestAppendEntriesReply struct {
	Term         int
	Success      bool
	LastLogIndex int
}

// TODO: here we maybe can make sure we send ConflictIndex and ConflictTerm instead of just commitID
// commit ID as a bit weird to send
// commit commandID is not persisted I think, so we might override commited entries by doing this?
func (rf *Raft) calculateConflictInfo(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.printLastLog("calculateConflictInfo")
	if len(rf.log) == 0 {
		reply.LastLogIndex = rf.log[0].Index - 1
		return
	}

	lastLogIndex := len(rf.log) - 1
	conflictIdx := min(args.PrevLogIndex-rf.log[0].Index, lastLogIndex)
	conflictTerm := rf.log[conflictIdx].Term

	commitIndex := rf.commitIndex - rf.log[0].Index
	for i := conflictIdx; commitIndex < i; i-- {
		log := rf.log[i]
		if log.Term == conflictTerm {
			reply.LastLogIndex = i
		} else {
			break
		}
	}
	DPrintf("IteratedCount=%d, LogLen=%d\n", conflictIdx-reply.LastLogIndex, len(rf.log))
	if rf.commitIndex > reply.LastLogIndex-rf.log[0].Index {
		// This means we are sending something back that is actually further back than our commit index?
		// this seems like it would be very bad
		// this is also a potential problem area i guess
		DPrintf("[1.calculateConflictInfo.%d.%d]: PrevLogIndex=%d LastLogIndex=%d Leader=%d\n", rf.currentTerm, rf.me, args.PrevLogIndex, lastLogIndex, args.Leader)
		DPrintf("[2.calculateConflictInfo.%d.%d]: LastLogIndex=%d commitCommandIndex=%d Leader=%d\n", rf.currentTerm, rf.me, reply.LastLogIndex, rf.commitIndex-1, args.Leader)
		reply.LastLogIndex = rf.commitIndex
	}
	DPrintf("[calculateConflictInfo.%d.%d]: PrevLogIndex=%d SendingLastLogIndex=%d Leader=%d\n", rf.currentTerm, rf.me, args.PrevLogIndex, lastLogIndex, args.Leader)
}

func (rf *Raft) denyAppendEntry(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	reply.Success = false
	reply.Term = rf.currentTerm
}

type RequestAppendEntriesArgs struct {
	Term              int
	Leader            int
	Entries           []LogEntry
	PrevLogIndex      int
	PrevLogTerm       int
	LeaderCommitIndex int
	LeaderCommitTerm  int
}

func (args RequestAppendEntriesArgs) leaderHasCommittedInOwnTerm() bool {
	return args.LeaderCommitTerm == args.Term
}

func (args RequestAppendEntriesArgs) hasEntries() bool {
	return len(args.Entries) > 0
}

func (rf *Raft) updateLastAppended(server int, recentCommandIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[server] = recentCommandIndex + 1
	rf.matchIndex[server] = recentCommandIndex
}

func (rf *Raft) storeCommitIdx(commitIndex int, commitTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex = commitIndex
	rf.commitTerm = commitTerm
}

func (rf *Raft) decrementNextIndex(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	decrementedIdx := min(rf.nextIndex[server]-1, reply.LastLogIndex)
	if decrementedIdx < 0 {
		decrementedIdx = 0
	}
	rf.nextIndex[server] = decrementedIdx
}

func (rf *Raft) sendEntries() {
	rf.mu.Lock()

	DPrintf("sendEntries START me=%d\n", rf.me)
	defer DPrintf("sendEntries DONE me=%d\n", rf.me)
	for i := rf.lastAppliedIndex + 1; i <= rf.commitIndex; i++ {
		if i <= rf.lastAppliedIndex {
			fmt.Println("How did this happen. Probs due to the lock.")
			continue
		}
		entry := rf.log[i-rf.log[0].Index]
		rf.lastAppliedIndex = entry.Index // TODO: fix deadlock
		rf.mu.Unlock()
		rf.applyCh <- entry.toApplyMsg()
		rf.mu.Lock()
		DPrintf("sendEntries me=%d entry=[%d]\n", rf.me, entry.Index)
	}
	// rf.lastAppliedIndex = rf.commitIndex // fix deadlock
	DPrintf("me=%d lastAppliedIndex=%d\n", rf.me, rf.lastAppliedIndex)
	rf.mu.Unlock()
}

func (rf *Raft) checkCommitted(recentCommandIndex int, recentCommandTerm int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d completed sending recentCommandIndex %d with Term %d. My current lastAppliedIndex is %d", rf.me, recentCommandIndex, recentCommandTerm, rf.lastAppliedIndex)
	DPrintf("[sendAppendEntries.term=%d.me=%d] Current commitIndex %d \n", rf.currentTerm, rf.me, rf.commitIndex)
	rf.printLastLog("checkCommitted")
	// Only commit replicated logs if we can confirm it is from the leaders current term
	if rf.currentTerm != recentCommandTerm {
		return false
	}
	// calculate new commit index
	replicatedCount := 1 // we know that this server has already replicated the log
	if recentCommandIndex > rf.commitIndex {
		// how many server have replicated this Command
		for _, index := range rf.matchIndex {
			if index >= recentCommandIndex {
				replicatedCount++
			}
		}
	}
	return replicatedCount > (len(rf.peers) / 2)
}

func (rf *Raft) getServerByID(server int) *labrpc.ClientEnd {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.peers[server]
}

func (rf *Raft) stepDown(term int) {
	DPrintf("[stepDown.%d.%d] Stepping down because I've seen a higher term than my current term.", rf.currentTerm, rf.me)
	rf.currentTerm = term
	rf.votedFor = nil
	rf.role = Follower
	rf.yesVotes = 0
	rf.totalVotes = 0
	rf.persist()
}

func (rf *Raft) stepDownWithLock(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.stepDown(term)
}

func (rf *Raft) sendAppendEntries(
	server int,
	args *RequestAppendEntriesArgs,
) {
	var reply *RequestAppendEntriesReply
	if ok := rf.getServerByID(server).Call("Raft.AppendEntries", args, &reply); !ok {
		// Invalid request: Was not able to reach server
		return
	}

	if term, _ := rf.GetState(); term > args.Term {
		// Invalid request: I am no longer in this term
		return
	}

	if !rf.isLeader() {
		// Invalid request: I am no longer the leader
		return
	}

	if !reply.Success {
		DPrintf("sendAppendEntries failed. me=%d LastLogIndex=%d, diffIndex=%d loglen=%d\n", rf.me, reply.LastLogIndex, rf.log[0].Index, len(rf.log))
		if term, _ := rf.GetState(); reply.Term > term {
			// The follower is in a higher term. Step down.
			rf.stepDownWithLock(reply.Term)
			return
		}
		rf.decrementNextIndex(server, args, reply)
		return
	}

	rf.mu.Lock()
	DPrintf("sendAppendEntries succeeded? me=%d target=%d entriesLen=%d LastLogIndex=%d, diffIndex=%d loglen=%d\n", rf.me, server, len(args.Entries), reply.LastLogIndex, rf.log[0].Index, len(rf.log))
	rf.mu.Unlock()

	if args.hasEntries() {
		recentEntry := args.Entries[len(args.Entries)-1]
		recentCommandTerm := recentEntry.Term
		recentCommandIndex := recentEntry.Index
		rf.updateLastAppended(server, recentCommandIndex)

		// update the commit idx
		if rf.checkCommitted(recentCommandIndex, recentCommandTerm) {
			rf.storeCommitIdx(recentCommandIndex, recentCommandTerm)
			// send freshly commited entries to the service throught the applyCh
			rf.sendEntries()
		}
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.role == Leader
	nextIndex := rf.getLastLogEntry().Index + 1
	if isLeader {
		rf.log = append(rf.log, LogEntry{
			Index:   nextIndex,
			Term:    term,
			Command: command,
		})
		rf.persist()
		DPrintf("[Start.%d.%d]: CommandIndex=%d Command=%s\n", term, rf.me, nextIndex, command)
	}
	return nextIndex, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendCommittedEntries() {
	for index := rf.lastAppliedIndex + 1; index <= rf.commitIndex; index++ {
		if index <= rf.lastAppliedIndex {
			fmt.Println("How did this happen. Probs due to the lock.")
			continue
		}
		entry := rf.log[index-rf.log[0].Index]
		DPrintf("sendCommittedEntries me=%d term=%d LogTerm=%d CommandIndex=%d\n", rf.me, rf.currentTerm,entry.Term, entry.Index)
		rf.lastAppliedIndex = entry.Index // TODO: fix deadlock
		rf.mu.Unlock()
		rf.applyCh <- entry.toApplyMsg()
		rf.mu.Lock()
	}
	rf.lastAppliedIndex = rf.commitIndex // fix deadlock and enable
	DPrintf("me=%d lastAppliedIndex=%d\n", rf.me, rf.lastAppliedIndex)
}

func (rf *Raft) appendNewEntries(args *RequestAppendEntriesArgs) {
	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	splitIdx := args.PrevLogIndex - rf.log[0].Index + 1
	DPrintf("appendNewEntries splitIdx=%d PrevLogIndex=%d diffIndex=%d loglen=%d\n", splitIdx, args.PrevLogIndex, rf.log[0].Index, len(rf.log))
	rf.log = rf.log[:splitIdx]
	rf.printLastLog("appendNewEntries(preMerge)")
	DPrintf("[appendNewEntries.%d.%d] Current CommitID %d \n", rf.currentTerm, rf.me, rf.commitIndex)
	entry := args.Entries[0]
	DPrintf("[appendNewEntries.%d.%d] First Appended Log [%d|%d]=%d", rf.currentTerm, rf.me, entry.Index, entry.Term, entry.Command)
	// 4. Append any new entries not already in the log
	rf.log = append(rf.log, args.Entries...)
}

func (rf *Raft) AppendEntries(
	args *RequestAppendEntriesArgs,
	reply *RequestAppendEntriesReply,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(
		"[AppendEntries.term=%d.me=%d] Received AppendEntries from leader=%d in term=%d NumEntries=%d PrevLogIndex=%d LeaderCommitIdx=%d mylastAppliedIndex=%d mycommitIndex=%d\n",
		rf.currentTerm,
		rf.me,
		args.Leader,
		args.Term,
		len(args.Entries),
		args.PrevLogIndex,
		args.LeaderCommitIndex,
		rf.lastAppliedIndex,
		rf.commitIndex,
	)
	// 1. Reply false if term < currentTerm (§5.1)
	if rf.currentTerm > args.Term {
		DPrintf("me=%d Not accepting log due to low args.Term=%d myTerm=%d", rf.me, args.Term, rf.currentTerm)
		DPrintf("me=%d Not accepting log due to low args.Term=%d myTerm=%d\n", rf.me, args.Term, rf.currentTerm)
		rf.denyAppendEntry(args, reply)
		return
	}

	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex >= 0 {
		// only validate log if PrevLogIndex > 0
		lastLogSliceIdx := len(rf.log) - 1
		if args.PrevLogIndex-rf.log[0].Index > lastLogSliceIdx {
			DPrintf(
				"me=%d Not accepting log due to too high prev log index. loglen=%d diffIndex=%d args.PrevLogIndex=%d, leader commit ID %d\n",
				rf.me,
				len(rf.log),
				rf.log[0].Index,
				args.PrevLogIndex,
				args.LeaderCommitIndex,
			)
			rf.calculateConflictInfo(args, reply)
			rf.denyAppendEntry(args, reply)
			return
		}

		if args.PrevLogIndex >= rf.log[0].Index && rf.log[args.PrevLogIndex-rf.log[0].Index].Term != args.PrevLogTerm {
			DPrintf("me=%d Not accepting log due to incorrect PrevLogTerm term=%d prevLogTerm %d\n", rf.me, rf.log[args.PrevLogIndex-rf.log[0].Index].Term, args.PrevLogTerm)
			rf.calculateConflictInfo(args, reply)
			rf.denyAppendEntry(args, reply)
			return
		}
	}

	if args.PrevLogIndex < rf.log[0].Index-1{
		panic("Invalid AppendEntriesRPC: Leader should send snapshot.")
	}

	// if args.PrevLogIndex == rf.log[0].Index {
	// 	panic("Invalid AppendEntriesRPC: Leader trying to replace my base idx for no reason.")
	// }

	// NOTE: Successful case
	reply.Success = true

	if args.hasEntries() {
		DPrintf("me=%d Appending entries to log PrevLogIndex=%d diffIndex=%d\n", rf.me, args.PrevLogIndex, rf.log[0].Index)
		// only append entries if PrevLogIndex is in the current log
		DPrintf("[appendNewEntries.%d.%d]", rf.currentTerm, rf.me)
		rf.appendNewEntries(args)
	}

	// Persist after appending entry but before sending to service
	// if raft crashes after sending to the service but not persisting the new log, we will have an undefined state
	rf.role = Follower
	rf.leader = &args.Leader
	rf.lastAppliedTime = time.Now()
	rf.persist()

	// only commit entries if the leader has commited entries in it's own term
	if args.LeaderCommitTerm == args.Term {
		if args.LeaderCommitIndex > rf.commitIndex {
			entry := rf.log[len(rf.log)-1]
			rf.commitIndex = min(entry.Index, args.LeaderCommitIndex)
		}
		rf.sendCommittedEntries()
		// 5. If leaderCommit > commitIndex, set commitIndex =
		// min(leaderCommit, index of last new entry)
	}
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role == Leader
}

func (rf *Raft) buildAppendEntriesArgs(i int) *RequestAppendEntriesArgs {
	leader := rf.me
	term := rf.currentTerm
	var (
		args RequestAppendEntriesArgs
	)
	nextIndex := rf.nextIndex[i]
	if nextIndex <= (len(rf.log) + rf.log[0].Index) {
		args.Entries = rf.log[nextIndex-rf.log[0].Index:]
	} else {
		panic("First: We should be sending snapshot right?")
	}
	args.LeaderCommitIndex = rf.commitIndex
	args.LeaderCommitTerm = rf.commitTerm
	args.Leader = leader
	args.Term = term
	args.PrevLogIndex = nextIndex - 1
	DPrintf("me=%d server=%d loglen=%d PrevLogIndex=%d diffIndex=%d\n", rf.me, i, len(rf.log), args.PrevLogIndex, rf.log[0].Index)
	if args.PrevLogIndex-rf.log[0].Index >= 0 {
		args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.log[0].Index].Term
	} else {
		panic("Second: We should be sending snapshot right?")
	}
	return &args
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastAppliedTime = time.Now()
	for i := range rf.peers {
		if i != rf.me {
			if nextIndex := rf.nextIndex[i]; nextIndex > rf.log[0].Index {
				go rf.sendAppendEntries(i, rf.buildAppendEntriesArgs(i))
			} else {
				go rf.sendInstallSnapshot(i, rf.buildInstallSnapshotArgs())
			}
		}
	}
}

func (rf *Raft) initLeaderState() {
	rf.role = Leader
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
}

func (rf *Raft) runLeader() {
	rf.initLeaderState()
	for !rf.killed() && rf.isLeader() {
		rf.sendHeartbeat()
		time.Sleep(time.Millisecond * time.Duration(35))
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.yesVotes=1 // vote for yourself
	rf.totalVotes=1 // vote for yourself
	rf.currentTerm++
	rf.votedFor = &rf.me
	rf.lastAppliedTime = time.Now()
	for i := range rf.peers {
		if i != rf.me {
			var (
				args  RequestVoteArgs
				reply RequestVoteReply
			)
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.CandidateCommitIndex = rf.commitIndex
			lastLog := rf.getLastLogEntry()
			args.PrevLogIndex = lastLog.Index
			args.PrevLogTerm = lastLog.Term
			go rf.sendRequestVote(i, &args, &reply)
		}
	}
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
	min := 200
	max := 500
	return time.Millisecond * time.Duration(rand.Intn(max-min)+min)
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

func (rf *Raft) runCandidate() {
	go rf.startElection()
	select {
		case <- rf.electionCh:
		case <-time.After(getRandomTickerDuration()):
	}
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

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.log = []LogEntry{{
		Term:    0,
		Index:   0,
		Command: nil,
	}}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = nil
	rf.lastAppliedTime = time.Time{}
	rf.lastAppliedIndex = 0
	rf.applyCh = applyCh
	rf.electionCh = make(chan ElectionResult)
	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()
	return rf
}
