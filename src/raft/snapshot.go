package raft

import "fmt"

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
// Previously, this lab recommended that you implement a function called CondInstallSnapshot to avoid
// the requirement that snapshots and log entries sent on applyCh are coordinated.
// This vestigal API interface remains,
// but you are discouraged from implementing it: instead, we suggest that you simply have it return true.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(snapshotIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if snapshotIndex <= rf.log[0].Index {
		DPrintf("INVALID INDEX (low) index=%d diffIndex=%d", snapshotIndex, rf.log[0].Index)
		panic("INVALID index (low)")
		return
	}

	if snapshotIndex-rf.log[0].Index > len(rf.log) {
		DPrintf("INVALID INDEX (high) index=%d diffIndex=%d logLen=%d", snapshotIndex, rf.log[0].Index, len(rf.log))
		panic("INVALID index (high)")
		return
	}

	if snapshotIndex > rf.commitIndex {
		DPrintf("INVALID INDEX (high) snapshotIndex=%d commitIndex=%d lastAppliedIndex=%d", snapshotIndex, rf.commitIndex, rf.lastAppliedIndex)
		fmt.Printf("INVALID INDEX (high) snapshotIndex=%d commitIndex=%d lastAppliedIndex=%d\n", snapshotIndex, rf.commitIndex, rf.lastAppliedIndex)
		// panic(fmt.Sprintf("INVALID INDEX (high) snapshotIndex=%d commitIndex=%d", snapshotIndex, rf.commitIndex))
		return
	}

	DPrintf("Snapshot changing diffIndex from=%d to=%d me=%d logLen=%d\n", rf.log[0].Index, snapshotIndex, rf.me, len(rf.log))
	rf.log = rf.log[snapshotIndex-rf.log[0].Index:]

	if rf.log[0].Index != snapshotIndex {
		panic("Snapshot: LastIncludedIndex is not equal to what service sent")
	}
	rf.persister.SaveStateAndSnapshot(rf.getRaftState(), snapshot)
}

type InstallSnapshotArgs struct {
	Term              int
	Leader            int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

func (args InstallSnapshotArgs) toApplyMsg() ApplyMsg {
	return ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

func (rf *Raft) buildInstallSnapshotArgs() *InstallSnapshotArgs {
	leader := rf.me
	term := rf.currentTerm
	var (
		args InstallSnapshotArgs
	)
	args.Term = term
	args.Leader = leader
	args.LastIncludedIndex = rf.log[0].Index
	args.LastIncludedTerm = rf.log[0].Term
	args.Data = rf.persister.ReadSnapshot()
	return &args
}

func (rf *Raft) isInLog(index int) bool {
	if index > rf.log[len(rf.log)-1].Index {
		return false
	}
	if index < rf.log[0].Index {
		return false
	}
	return true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		// 1. Reply immediately if term < currentTerm
		DPrintf("InstallSnapshot: me=%d denying snapshot due to low term=%d myterm=%d\n", rf.me, args.Term, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		rf.mu.Unlock()
		rf.stepDown(args.Term)
		rf.mu.Lock()
	}

	// if rf.lastAppliedIndex < rf.commitIndex {
	// 	fmt.Println("NOT applying stale snapshot")
	// 	return
	// }

	// 6. If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply

	if rf.isInLog(args.LastIncludedIndex) {
		// keep the log entries after LastIncludedIndex
		// rf.log = append(log, [])
		rf.log = rf.log[args.LastIncludedIndex-rf.log[0].Index:]
		if rf.log[0].Index != args.LastIncludedIndex {
			panic("InstallSnapshot: LastIncludedIndex is not equal to what the leader sent")
		}
	} else {
		// discard the entire log after LastIncludedIndex
		rf.log = []LogEntry{{
			Term:    args.LastIncludedTerm,
			Index:   args.LastIncludedIndex,
			Command: nil,
		}}
	}

	rf.persister.SaveStateAndSnapshot(rf.getRaftState(), args.Data)

	fmt.Printf(
		"InstallSnapshot: me=%d lastAppliedIndex=%d LastIncludedIndex=%d commitIndex=%d\n", rf.me, rf.lastAppliedIndex, args.LastIncludedIndex, rf.commitIndex,
	)

	rf.lastAppliedIndex = args.LastIncludedIndex

	rf.commitIndex = max(args.LastIncludedIndex, rf.commitIndex)

	// 8. Reset state machine using snapshot contents (and load
	// snapshot’s cluster configuration)

	rf.mu.Unlock()
	rf.applyCh <- args.toApplyMsg()
	rf.mu.Lock()
	reply.Success = true
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) {
	DPrintf("sendInstallSnapshot: me=%d server=%d LastIncludedIndex=%d \n", rf.me, server, args.LastIncludedIndex)
	var reply *InstallSnapshotReply
	if ok := rf.getServerByID(server).Call("Raft.InstallSnapshot", args, &reply); !ok {
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

	if term, _ := rf.GetState(); reply.Term > term {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.stepDown(reply.Term)
		return
	}

	if reply.Success {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}
}
