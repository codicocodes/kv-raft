package raft

// CommitHash: Refactor PrevLogIndex to PrevCommandID: 53c8fed
// Attempt at SendSnapshotRPC af800a40cd7a0f3526dd438c6e8b067b42e336a9

import (
	"bytes"
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
}

func (rf *Raft) getLastLogEntry() LogEntry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.leader == &rf.me
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	if rf.votedFor != nil {
		e.Encode(rf.votedFor)
	}
	e.Encode(rf.currentTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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

	// handle candidate is in same term
	if rf.checkAlreadyVotedInTerm(args) {
		// candidate is in same term as me but I already voted
		DPrintf("Voting no because me=%d already voted in term %d, my term %d", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// handle candidate is in a higher term

	// the term has increased, or we did not vote in this term
	// if I did not vote in this term, I should not be the leader
	// so either way, we should step down from leadership here
	// make sure we step down from leadership if we see a higher term in the voting process
	// we used to not do this, and it's possible that this was the cause of a bug
	// do we do this even if we do not grant the vote? because we saw a higher term

	rf.mu.Unlock()
	rf.stepDown(args.Term)
	rf.mu.Lock()

	logLength := len(rf.log)

	// validate if candidates log is up to date

	if logLength == 0 {
		// if my log is empty the candidates log has gotta be at least as up to date DPrintf("Voting yes because my log is empty so candidate is up to date")
		rf.grantVote(args, reply)
		return
	}

	lastLog := rf.getLastLogEntry()

	lastLogTerm := lastLog.Term

	// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	if lastLogTerm > args.PrevLogTerm {
		// candidates last log term is lower, voting no
		rf.printLastLog("VoteNo")
		DPrintf("%d Voting no because my last entry has higher term %d, candiate PrevLogTerm %d", rf.me, lastLogTerm, args.PrevLogTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	lastLogIndex := lastLog.Index

	// If the logs end with the same term
	// whichever log is longer is more up-to-date
	if lastLogTerm == args.PrevLogTerm && lastLogIndex > args.PrevLogIndex {
		rf.printLastLog("VoteNo")
		DPrintf("%d Voting no because my log lastLogIndex is larger %d, candidate PrevLogIndex %d", rf.me, lastLogIndex, args.PrevLogIndex)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.grantVote(args, reply)
}

func (rf *Raft) sendRequestVote(
	server int,
	args *RequestVoteArgs,
	reply *RequestVoteReply,
	voteCh chan int,
) {
	if ok := rf.getServerByID(server).Call("Raft.RequestVote", args, reply); !ok {
		// Invalid Request: Not able to reach server. Equivalent to NO vote.
		voteCh <- 0
		return
	}

	if term, _ := rf.GetState(); reply.Term > term {
		// The server responded with a higher term. I am stepping down from candidate role.
		rf.stepDown(reply.Term)
		voteCh <- -1
		return
	}

	var vote int

	if reply.VoteGranted {
		// The server granted the vote
		vote = 1
	}

	voteCh <- vote
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
	rf.printLastLog("denyAppendEntry")
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

	log := rf.log
	diffIdx := rf.log[0].Index
	nextIndexToApply := rf.lastAppliedIndex + 1

	DPrintf("me=%d nextIndexToApply=%d diffIndex=%d commitIndex=%d\n", rf.me, nextIndexToApply, rf.log[0].Index, rf.commitIndex)
	for i := nextIndexToApply; i <= rf.commitIndex; i++ {
		entry := log[i-diffIdx]

		rf.mu.Unlock()
		rf.applyCh <- entry.toApplyMsg()
		rf.mu.Lock()
		DPrintf("sendEntries me=%d entry=[%d]\n", rf.me, entry.Index)
	}
	rf.lastAppliedIndex = rf.commitIndex
	DPrintf("me=%d lastAppliedIndex=%d\n", rf.me, rf.lastAppliedIndex)
	rf.mu.Unlock()
}

func (rf *Raft) checkCommitted(recentCommandIndex int, recentCommandTerm int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d completed sending recentCommandIndex %d with Term %d. My current lastAppliedIndex is %d", rf.me, recentCommandIndex, recentCommandTerm, rf.lastAppliedIndex)
	DPrintf("[sendAppendEntries.%d.%d] Current commitIndex %d \n", rf.currentTerm, rf.me, rf.commitIndex)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[stepDown.%d.%d] Stepping down because I've seen a higher term than my current term.", rf.currentTerm, rf.me)
	rf.currentTerm = term
	rf.votedFor = nil
	rf.role = Follower
	rf.persist()
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
			rf.stepDown(reply.Term)
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
			// send freshly commited entries to the service throught the applyCh
			rf.storeCommitIdx(recentCommandIndex, recentCommandTerm)
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

func (rf *Raft) sendCommittedEntries(commitIndex int) {
	for index := rf.lastAppliedIndex + 1; index <= commitIndex; index++ {
		if index-rf.log[0].Index >= len(rf.log) {
			// 98 - 89 >= 1
			// 10 >= 1
			// panic
			DPrintf("sendCommittedEntries (panic) lastAppliedIndex=%d diffIdx=%d commitIndex=%d loglen=%d\n", rf.lastAppliedIndex, rf.log[0].Index, commitIndex, len(rf.log))
			lastLog := rf.getLastLogEntry()
			DPrintf("CommandIndex=%d\n", lastLog.Index)
			panic("used to break here")
		}
		DPrintf("sendCommittedEntries me=%d index=%d diffIndex=%d loglen=%d\n", rf.me, index, rf.log[0].Index, len(rf.log))
		entry := rf.log[index-rf.log[0].Index]
		DPrintf("[sendCommittedEntries.%d.%d] Sending Log Entry [%d|%d]=%d", rf.currentTerm, rf.me, entry.Index, entry.Term, entry.Command)
		DPrintf("sendCommittedEntries me=%d CommandIndex=%d\n", rf.me, entry.Index)
		rf.mu.Unlock()
		rf.applyCh <- entry.toApplyMsg()
		rf.mu.Lock()
		rf.lastAppliedIndex = entry.Index
	}
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
		"[AppendEntries.%d.%d] Received AppendEntries from leader=%d in term=%d\n",
		rf.currentTerm,
		rf.me,
		args.Leader,
		args.Term,
	)
	// 1. Reply false if term < currentTerm (§5.1)
	if rf.currentTerm > args.Term {
		DPrintf("me=%d Not accepting log due to low args.Term=%d myTerm=%d", rf.me, args.Term, rf.currentTerm)
		DPrintf("me=%d Not accepting log due to low args.Term=%d myTerm=%d\n", rf.me, args.Term, rf.currentTerm)
		rf.denyAppendEntry(args, reply)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = nil
		rf.role = Follower
		rf.persist()
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex >= 0 {
		// only validate log if PrevLogIndex > 0
		lastLogSliceIdx := len(rf.log) - 1
		if args.PrevLogIndex-rf.log[0].Index > lastLogSliceIdx {
			DPrintf(
				"me=%d Not accepting log due to too high prev log index. loglen =%d, args.PrevLogIndex=%d, leader commit ID %d",
				rf.me,
				len(rf.log),
				args.PrevLogIndex,
				args.LeaderCommitIndex,
			)
			DPrintf(
				"me=%d Not accepting log due to too high prev log index. loglen=%d diffIndex=%d args.PrevLogIndex=%d, leader commit ID %d\n",
				rf.me,
				len(rf.log),
				rf.log[0].Index,
				args.PrevLogIndex,
				args.LeaderCommitIndex,
			)
			DPrintf("me=%d My commit ID: %d", rf.me, rf.commitIndex)
			rf.calculateConflictInfo(args, reply)
			rf.denyAppendEntry(args, reply)
			return
		}

		if args.PrevLogIndex >= rf.log[0].Index && rf.log[args.PrevLogIndex-rf.log[0].Index].Term != args.PrevLogTerm {
			DPrintf("me=%d Not accepting log due to incorrect PrevLogTerm my term %d, prevLogTerm %d", rf.me, rf.log[args.PrevLogIndex-rf.log[0].Index].Term, args.PrevLogTerm)
			DPrintf("me=%d Not accepting log due to incorrect PrevLogTerm my term %d, prevLogTerm %d\n", rf.me, rf.log[args.PrevLogIndex-rf.log[0].Index].Term, args.PrevLogTerm)
			DPrintf("me=%d My commit ID: %d", rf.me, rf.commitIndex)
			rf.calculateConflictInfo(args, reply)
			rf.denyAppendEntry(args, reply)
			return
		}

		if args.PrevLogIndex >= rf.log[0].Index {
			DPrintf(
				"[AppendEntries.%d.%d] finnished checking for failed cases PrevLogTerm=%d PrevLogIndex=%d LastLogCommandID=%d LastLogTerm=%d\n",
				rf.currentTerm,
				rf.me,
				args.PrevLogTerm,
				args.PrevLogIndex,
				rf.log[args.PrevLogIndex-rf.log[0].Index].Index,
				rf.log[args.PrevLogIndex-rf.log[0].Index].Term,
			)
			if rf.log[args.PrevLogIndex-rf.log[0].Index].Index != args.PrevLogIndex {
				panic("This should be the same right?")
			}
		}
	}

	if args.PrevLogIndex >= rf.log[0].Index-1 {
		DPrintf("me=%d Accepting log PrevLogIndex=%d diffIndex=%d\n", rf.me, args.PrevLogIndex, rf.log[0].Index)
		// NOTE: Successful case
		reply.Success = true

		if args.hasEntries() {
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
			rf.sendCommittedEntries(args.LeaderCommitIndex)
			// 5. If leaderCommit > commitIndex, set commitIndex =
			// min(leaderCommit, index of last new entry)
			if len(args.Entries) > 0 && args.LeaderCommitIndex > rf.commitIndex {
				lastEntry := args.Entries[len(args.Entries)-1]
				rf.commitIndex = min(lastEntry.Index, args.LeaderCommitIndex)
				// update commitCommandTerm should not be necessary
				// because no node can be leader in term 0
			}
		}
	} else {
		DPrintf("Why is this happening?\n")
		reply.Success = false
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
	}
	args.LeaderCommitIndex = rf.commitIndex
	args.LeaderCommitTerm = rf.commitTerm
	args.Leader = leader
	args.Term = term
	args.PrevLogIndex = nextIndex - 1
	DPrintf("me=%d server=%d loglen=%d PrevLogIndex=%d diffIndex=%d\n", rf.me, i, len(rf.log), args.PrevLogIndex, rf.log[0].Index)
	if args.PrevLogIndex-rf.log[0].Index >= 0 {
		args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.log[0].Index].Term
	}
	return &args
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastAppliedTime = time.Now()
	for i := range rf.peers {
		if i != rf.me {
			if nextIndex := rf.nextIndex[i]; nextIndex >= rf.log[0].Index {
				go rf.sendAppendEntries(i, rf.buildAppendEntriesArgs(i))
			} else {
				println("should send install snapshot?", nextIndex)
			}
		}
	}
}

func (rf *Raft) initLeaderState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.matchIndex = make([]int, len(rf.peers))
	if len(rf.log) > 0 {
		for i := range rf.matchIndex {
			rf.matchIndex[i] = 0
		}
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

func (rf *Raft) sendAllVoteRequests(voteCh chan int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
	term := rf.currentTerm
	rf.votedFor = &rf.me
	rf.lastAppliedTime = time.Now()
	DPrintf("[becomeCandidate.%d.%d] Start\n", rf.currentTerm, rf.me)
	for i := range rf.peers {
		if i != rf.me {
			var (
				args  RequestVoteArgs
				reply RequestVoteReply
			)
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			if len(rf.log) > 0 {
				args.CandidateCommitIndex = rf.commitIndex
				lastLog := rf.getLastLogEntry()
				args.PrevLogIndex = lastLog.Index
				args.PrevLogTerm = lastLog.Term
			}
			go rf.sendRequestVote(i, &args, &reply, voteCh)
		}
	}
	DPrintf("[becomeCandidate.%d.%d] Waiting\n", rf.currentTerm, rf.me)
	return term
}

func (rf *Raft) countVotes(voteCh chan int) int {
	electionTimeout := make(chan bool)
	totalVotes := 1
	yesVote := 1 // vote for myself

	go func() {
		time.Sleep(getRandomTickerDuration())
		electionTimeout <- true
	}()
	for {
		select {
		case <-electionTimeout:
			return yesVote
		case vote := <-voteCh:
			if vote < 0 {
				// we saw a higher term and stepped down from Candidate role
				// Stop waiting for votes and return 0 votes
				// consider refactoring voteCh to take enum
				return 0
			}
			yesVote += vote
			totalVotes++
			if yesVote > len(rf.peers)/2 {
				// we have a majority
				return yesVote
			}
			if totalVotes-yesVote > len(rf.peers)/2 {
				// no has a majority 😢
				return yesVote
			}
			if totalVotes == len(rf.peers) {
				// all servers voted
				return yesVote
			}
		}
	}
}

func (rf *Raft) electionOutcome(candidateTerm int, yesVotes int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	election_winner := yesVotes > len(rf.peers)/2

	// Do not take leadership if we are not in the candidate term
	if rf.currentTerm != candidateTerm {
		return
	}

	// Do not take leadership if you are no longer a candidate (maybe checking term is enough)
	// We used to get this case because we would step down from Candidacy because we had failed to replicate an old term
	// Should be fine since we stopped handling AppendEntriesCallbacks from previous terms
	// This should no longer happen, pending confirmation
	// another issue here is if we step down when we cannot decrement nextindex for a server
	// if there is a bug there this situation could happen
	if rf.role != Candidate {
		if election_winner {
			DPrintf(
				"[becomeCandidate.%d.%d] BUG: won=%v No longer Candidate. role=%s. candidateTerm=%d currTerm=%d",
				rf.currentTerm,
				rf.me,
				election_winner,
				rf.role.String(),
				candidateTerm,
				rf.currentTerm,
			)
			DPrintf(
				"[becomeCandidate.%d.%d] BUG: won=%v No longer Candidate. role=%s. candidateTerm=%d leader=%d\n",
				rf.currentTerm,
				rf.me,
				election_winner,
				rf.role.String(),
				candidateTerm,
				*rf.leader,
			)
		}
		return
	}

	if election_winner {
		DPrintf("[becomeCandidate.%d.%d] Taking leadership term=%d\n", rf.currentTerm, rf.me, rf.currentTerm)
		rf.leader = &rf.me
		rf.role = Leader
		return
	}
	rf.role = Follower
	DPrintf("[becomeCandidate.%d.%d] Lost election. Stepping down to follower term=%d\n", rf.currentTerm, rf.me, rf.currentTerm)
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
	voteCh := make(chan int)
	candidateTerm := rf.sendAllVoteRequests(voteCh)
	yesVotes := rf.countVotes(voteCh)
	rf.electionOutcome(candidateTerm, yesVotes)
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
	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()
	return rf
}
