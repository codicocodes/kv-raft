package raft

import (
	//	"bytes"
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
	lastAppliedIndex  int
	role         Role
	applyCh      chan ApplyMsg
	log          []ApplyMsg
	commitCommandID  int
	commitCommandTerm  int
	nextIndex    []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchCommandIds   []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

func (rf *Raft) printLastLog(fnName string)  {
	if len(rf.log) > 0 {
		entry := rf.log[len(rf.log) - 1]
		DPrintf("[%s.%d.%d] Last Command In Log [%d|%d]=%d", fnName, rf.currentTerm, rf.me, entry.CommandIndex, entry.Term, entry.Command)
	}
}


func (rf *Raft) getLastLogEntry() (*ApplyMsg) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	if len(rf.log) == 0 {
		return nil
	}
	return &rf.log[len(rf.log) - 1]
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
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
	var log []ApplyMsg
	var votedFor *int
	var currentTerm int
	if e := d.Decode(&log); e != nil {
		println(e)
		panic("Fail decode log")
	} 
	if e := d.Decode(&votedFor); e != nil {
		println(e)
		panic("Fail decode votedFor")
	} 
	if e := d.Decode(&currentTerm); e != nil {
		println("Fail decode currentTerm", e.Error())
		currentTerm = 0
	}
	rf.log = log
	rf.votedFor = votedFor
	rf.currentTerm = currentTerm
}


// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Previously, this lab recommended that you implement a function called CondInstallSnapshot to avoid 
	// the requirement that snapshots and log entries sent on applyCh are coordinated. 
	// This vestigal API interface remains,
	// but you are discouraged from implementing it: instead, we suggest that you simply have it return true.
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {

}


type RequestVoteArgs struct {
	CandidateId       int
	CandidateCommitID int
	Term              int
	PrevCommandID     int
	PrevLogTerm       int
}

type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

func (reply *RequestVoteReply) VoteNo(term int) {
	reply.Term = term
	reply.VoteGranted = false
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


func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		DPrintf("Voting no due to low candiate term %d, my term %d", args.Term, rf.currentTerm)
		reply.VoteNo(rf.currentTerm)
		return
	}

	// if votedFor is null it indicates its the first vote
	if rf.votedFor == nil {
		DPrintf("Voting yes due to never voted before in my life")
		rf.grantVote(args, reply)
		return
	}

	logLength := len(rf.log)

	// if there is nothing in the log the voters log can't be more up to date
	if logLength == 0 {
		DPrintf("Voting yes because nothing my log so candidat egotta be up to date")
		rf.grantVote(args, reply)
		return
	}

	lastLog := rf.log[logLength - 1]

	lastLogTerm := lastLog.Term

	// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	if lastLogTerm > args.PrevLogTerm {
		rf.printLastLog("VoteNo")
		DPrintf("%d Voting no because my last entry has higher term %d, candiate PrevLogTerm %d", rf.me, lastLogTerm, args.PrevLogTerm)
		reply.VoteNo(lastLogTerm)
		return
	}

	lastLogCommandID := lastLog.CommandIndex

	// If the logs end with the same term, then whichever log is longer is more up-to-date
	if lastLogTerm == args.PrevLogTerm {
		if lastLogCommandID > args.PrevCommandID {
			rf.printLastLog("VoteNo")
			DPrintf("%d Voting no because my log lastCommandID is larger %d, candidate PrevCommandID %d", rf.me, lastLogCommandID, args.PrevCommandID)
			reply.VoteNo(lastLogTerm)
			return
		}
	}

	rf.grantVote(args, reply)
}

func (rf *Raft) sendRequestVote(
	server int,
	args *RequestVoteArgs,
	reply *RequestVoteReply,
	voteCh chan int,
) {
	ok := rf.getServerByID(server).Call("Raft.RequestVote", args, reply)
	if !ok {
		return
	}
	if reply.VoteGranted {
		voteCh <- 1
	} else {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.role = Follower
			rf.persist()
		}
		rf.mu.Unlock()
		voteCh <- 0
	}
}

type RequestAppendEntriesReply struct {
	Term    int
	Success bool
	LastLogIndex int
}


// TODO: here we maybe can make sure we send ConflictIndex and ConflictTerm instead of just commitID
// commit ID as a bit weird to send
// commit commandID is not persisted I think, so we might override commited entries by doing this?
func (rf *Raft) calculateConflictInfo(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply){
	rf.printLastLog("calculateConflictInfo")
	// TODO: Conflicting index should be the first index of that specific problematic term

	if len(rf.log) == 0 {
		reply.LastLogIndex = -1
		return
	}

	lastLogIndex := len(rf.log) - 1
	lastLog := rf.log[lastLogIndex]

	if args.PrevLogIndex > lastLogIndex && lastLog.Term != args.PrevLogTerm{
		reply.LastLogIndex = lastLogIndex
		return
	}

	conflictIdx := min(args.PrevLogIndex, lastLogIndex)
	conflictTerm := rf.log[conflictIdx].Term

	for i := conflictIdx; rf.commitCommandID < i; i-- {
		log := rf.log[i]
		if log.Term == conflictTerm {
			reply.LastLogIndex = i
		} else {
			break
		}
	}
	DPrintf("IteratedCount=%d, LogLen=%d\n", conflictIdx - reply.LastLogIndex, len(rf.log))
	if rf.commitCommandID - 1 > reply.LastLogIndex {
		// This means we are sending something back that is actually further back than our commit index?
		// this seems like it would be very bad
		DPrintf("[1.calculateConflictInfo.%d.%d]: PrevLogIndex=%d LastLogIndex=%d Leader=%d\n", rf.currentTerm, rf.me, args.PrevLogIndex, lastLogIndex, args.Leader)
		fmt.Printf("[2.calculateConflictInfo.%d.%d]: LastLogIndex=%d commitCommandIndex=%d Leader=%d\n", rf.currentTerm, rf.me, reply.LastLogIndex, rf.commitCommandID - 1, args.Leader)
		reply.LastLogIndex = rf.commitCommandID
	}
	DPrintf("[calculateConflictInfo.%d.%d]: PrevLogIndex=%d SendingLastLogIndex=%d Leader=%d\n", rf.currentTerm, rf.me, args.PrevLogIndex, lastLogIndex, args.Leader)
}

func (rf *Raft) denyAppendEntry(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply){
	rf.printLastLog("denyAppendEntry")
	reply.Success = false
	reply.Term = rf.currentTerm
}

type RequestAppendEntriesArgs struct {
	Term         int
	Leader       int
	Entries      []ApplyMsg
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommitID int
	LeaderCommitTerm int
}

func (args RequestAppendEntriesArgs) hasEntries() bool {
	return len(args.Entries) > 0
}

func (rf *Raft) updateLastAppended(server int, recentCommandID int){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[server] = recentCommandID
	rf.matchCommandIds[server] = recentCommandID
}

func (rf *Raft) storeCommitIdx(commandID int, commandTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitCommandID = commandID
	rf.commitCommandTerm = commandTerm
}

func (rf *Raft) decrementNextIndex(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	decrementedIdx := min(rf.nextIndex[server] - 1, reply.LastLogIndex)
	if decrementedIdx < 0 {
		decrementedIdx = 0
	}
	nextIndex := rf.nextIndex[server]
	if nextIndex <= decrementedIdx {
		fmt.Printf("nextIndex=%d decrementedNextIdx=%d\n", nextIndex, decrementedIdx)
		panic("nextIndex <= decrementedIdx LMAO")
	}
	rf.nextIndex[server] = decrementedIdx
}

func (rf *Raft) sendEntries(recentCommandID int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	commitIdx := int(rf.commitCommandID)
	for index := commitIdx; index < recentCommandID; index++ {
		entry := rf.log[index]
		rf.applyCh <- entry
		rf.lastAppliedIndex = entry.CommandIndex - 1
	}
}

func (rf *Raft) checkCommitted(recentCommandID int, recentCommandTerm int) bool{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d completed sending recentCommandID %d with Term %d. My current lastAppliedIndex is %d", rf.me, recentCommandID, recentCommandTerm, rf.lastAppliedIndex)
	DPrintf("[sendAppendEntries.%d.%d] Current CommitID %d \n", rf.currentTerm, rf.me, rf.commitCommandID)
	rf.printLastLog("checkCommitted")
	// NOTE: Only commit replicated logs if we can confirm it is from the leaders current term
	if rf.currentTerm != recentCommandTerm {
		return false
	}
	// NOTE: calculate new commit index
	replicatedCount := 1 // we know that this server has already replicated the log
	if recentCommandID > rf.commitCommandID {
		// NOTE: how many server have replicated this Command
		for _, matchID := range rf.matchCommandIds {
			if matchID >= recentCommandID {
				replicatedCount++
			}
		}
	}
	return replicatedCount > (len(rf.peers) / 2)
}

func (rf *Raft) getServerByID(server int)*labrpc.ClientEnd {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.peers[server]
}


func (rf *Raft) sendAppendEntries(
	server int, 
	args *RequestAppendEntriesArgs, 
) {
	var reply *RequestAppendEntriesReply
	if ok := rf.getServerByID(server).Call("Raft.AppendEntries", args, &reply); !ok {
		return
	}

	if !reply.Success {
		if reply.Term > rf.currentTerm {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.currentTerm = reply.Term
			rf.role = Follower
			rf.persist()
		} else {
			rf.decrementNextIndex(server, args, reply)
		}
		return
	}

	if args.hasEntries() {
		recentEntry := args.Entries[len(args.Entries) - 1]
		recentCommandTerm := recentEntry.Term
		recentCommandID := recentEntry.CommandIndex
		rf.updateLastAppended(server, recentCommandID)

		// NOTE: update the commit idx
		if rf.checkCommitted(recentCommandID, recentCommandTerm) {
			// NOTE: send freshly commited entries to the applyCh
			rf.sendEntries(recentCommandID)
			rf.storeCommitIdx(recentCommandID, recentCommandTerm)
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
	index := len(rf.log) + 1
	term := rf.currentTerm
	isLeader := rf.role == Leader
	if isLeader {
		entry := rf.getLastLogEntry()
		if entry != nil {
			index = entry.CommandIndex + 1
		}
		rf.log = append(rf.log, ApplyMsg{
			Term: term,
			CommandValid: true,
			Command: command,
			CommandIndex: index,
		})
		rf.persist()
		DPrintf("[Start.%d.%d]: CommandIndex=%d Command=%s\n", term, rf.me, index, command)
	}
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


func (rf *Raft) sendCommittedEntries(commitID int) {
	for index := rf.lastAppliedIndex; index < commitID; index++ {
		if index >= len(rf.log) {
			break
		}
		msg := rf.log[index]
		rf.applyCh <- msg
		rf.lastAppliedIndex = msg.CommandIndex - 1
	}
}

func (rf *Raft) appendNewEntries(args *RequestAppendEntriesArgs) {
	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	splitIdx := args.PrevLogIndex + 1
	rf.log = rf.log[:splitIdx]
	rf.printLastLog("appendNewEntries")
	DPrintf("[appendNewEntries.%d.%d] Current CommitID %d \n", rf.currentTerm, rf.me, rf.commitCommandID)
	// 4. Append any new entries not already in the log
	rf.log = append(rf.log, args.Entries...)
}


func (rf *Raft) AppendEntries(
	args *RequestAppendEntriesArgs, 
	reply *RequestAppendEntriesReply,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1. Reply false if term < currentTerm (§5.1)
	if rf.currentTerm > args.Term {
		DPrintf("%d Not accepting log due to low term %d myTerm=%d", rf.me, args.Term, rf.currentTerm)
		rf.denyAppendEntry(args, reply)
		return
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex >= 0 {
		lastLogIndex := len(rf.log) - 1
		if lastLogIndex < args.PrevLogIndex {
			DPrintf("%d Not accepting log due to too high prev log index. loglen %d, prev index %d, leader commit ID %d", rf.me, len(rf.log), args.PrevLogIndex, args.LeaderCommitID)
			DPrintf("%d My commit ID: %d", rf.me, rf.commitCommandID)
			rf.calculateConflictInfo(args, reply)
			rf.denyAppendEntry(args, reply)
			return
		}

		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			DPrintf("%d Not accepting log due to incorrect PrevLogTerm my term %d, prevLogTerm %d", rf.me, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
			DPrintf("%d My commit ID: %d", rf.me, rf.commitCommandID)
			rf.calculateConflictInfo(args, reply)
			rf.denyAppendEntry(args, reply)
			return
		} 
		DPrintf(
			"[appendNewEntries.%d.%d] PrevLogTerm=%d PrevLogIndex=%d LastLogCommandID=%d LastLogTerm=%d\n",
			rf.currentTerm, 
			rf.me,
			args.PrevLogTerm,
			args.PrevLogIndex,
			rf.log[args.PrevLogIndex].CommandIndex,
			rf.log[args.PrevLogIndex].Term,
		)
		if rf.log[args.PrevLogIndex].CommandIndex - 1 != args.PrevLogIndex {
			panic("THIS SHOULd BE THe SAME rightr?")
		}
	}

	// NOTE: Successful case
	reply.Success = true

	if args.hasEntries() {
		DPrintf("[appendNewEntries.%d.%d] ", rf.currentTerm, rf.me)
		rf.appendNewEntries(args)
	}

	// Persist after appending entry but before sending to service
	// if raft crashes after sending to the service but not persisting the new log, we will have an undefined state
	rf.role = Follower
	rf.leader = &args.Leader
	rf.currentTerm = args.Term
	rf.lastAppliedTime = time.Now()
	rf.persist()

	// NOTE: only commit entries if the leader has commited entries in it's own term
	if args.LeaderCommitTerm == args.Term {
		rf.sendCommittedEntries(args.LeaderCommitID)
		// 5. If leaderCommit > commitIndex, set commitIndex =
		// min(leaderCommit, index of last new entry)
		if len(args.Entries) > 0 && args.LeaderCommitID > rf.commitCommandID {
			lastEntry := args.Entries[len(args.Entries) - 1]
			rf.commitCommandID = min(lastEntry.CommandIndex, args.LeaderCommitID)
			// update commitCommandTerm should not be necessary
			// because no node can be leader in term 0
		}
	}
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role == Leader
}
func (rf *Raft) buildAppendEntriesArgs(i int) (
	*RequestAppendEntriesArgs, 
) {
	leader := rf.me
	term := rf.currentTerm
	var (
		args RequestAppendEntriesArgs
	)
	nextIndex := rf.nextIndex[i]
	if nextIndex <= len(rf.log) {
		args.Entries = rf.log[nextIndex:]
	}
	args.LeaderCommitID = rf.commitCommandID
	args.LeaderCommitTerm = rf.commitCommandTerm
	args.Leader = leader
	args.Term = term
	args.PrevLogIndex = nextIndex - 1
	if len(rf.log) > args.PrevLogIndex && args.PrevLogIndex >= 0 {
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	}
	return &args
}

func (rf *Raft) sendHeartbeat()  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastAppliedTime = time.Now()
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendAppendEntries(i, rf.buildAppendEntriesArgs(i))
		}
	}
}


func (rf *Raft) initLeaderState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.matchCommandIds = make([]int, len(rf.peers))
	if len(rf.log) > 0 {
		for i := range rf.matchCommandIds {
			rf.matchCommandIds[i] = 0
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

func (rf *Raft) sendAllVoteRequests(voteCh chan int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
	rf.votedFor = &rf.me
	rf.lastAppliedTime = time.Now()
	DPrintf("[becomeCandidate.%d.%d] Start\n", rf.currentTerm, rf.me)
	for i := range rf.peers {
		if i != rf.me {
			var (
				args RequestVoteArgs
				reply RequestVoteReply
			)				
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.CandidateCommitID = rf.commitCommandID
			if len(rf.log) > 0 {
				lastLog := rf.getLastLogEntry()
				args.PrevCommandID = lastLog.CommandIndex
				args.PrevLogTerm = lastLog.Term
			}
			go rf.sendRequestVote(i, &args, &reply, voteCh)
		}
	}
	DPrintf("[becomeCandidate.%d.%d] Waiting\n", rf.currentTerm, rf.me)
}

func (rf *Raft) countVotes(voteCh chan int) int  {
	voteOver := make(chan bool)
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
		DPrintf("[becomeCandidate.%d.%d] Taking leadership\n", rf.currentTerm, rf.me)
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
	min := 200
	max := 400
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

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = nil
	rf.lastAppliedTime = time.Time{}
	rf.applyCh = applyCh
	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()
	return rf
}
