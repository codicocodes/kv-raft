package raft

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	/*
		1. ✅	A good place to start is to modify your code to so that it is
			able to store the part of the log starting at some index X.
			Initially you can set X to zero and run the 2B/2C tests.
			Then make Snapshot(index) discard the log before index, and set X equal to index.
			If all goes well you should now pass the first 2D test.
	*/

	// 2. ✅ won't be able to store the log in a Go slice and use Go slice indices interchangeably with Raft log indices; you'll need to index the slice in a way that accounts for the discarded portion of the log.

	// 3. Next: have the leader send an InstallSnapshot RPC if it doesn't have the log entries required to bring a follower up to date.

	// 4. Send the entire snapshot in a single InstallSnapshot RPC. Don't implement Figure 13's offset mechanism for splitting up the snapshot.

	// 5. Raft must discard old log entries in a way that allows the Go garbage collector to free and re-use the memory; this requires that there be no reachable references (pointers) to the discarded log entries.

	// 6. Even when the log is trimmed, your implemention still needs to properly send the term and index of the entry prior to new entries in AppendEntries RPCs; this may require saving and referencing the latest snapshot's lastIncludedTerm/lastIncludedIndex (consider whether this should be persisted).

	// 7. A reasonable amount of time to consume for the full set of Lab 2 tests (2A+2B+2C+2D) without -race is 6 minutes of real time and one minute of CPU time. When running with -race, it is about 10 minutes of real time and two minutes of CPU time.

	if index <= rf.log[0].Index {
		DPrintf("INVALID INDEX (low) index=%d diffIndex=%d", index, rf.log[0].Index)
		return
	}

	if index-rf.log[0].Index > len(rf.log) {
		DPrintf("INVALID INDEX (high) index=%d diffIndex=%d logLen=%d", index, rf.log[0].Index, len(rf.log))
		return
	}

	DPrintf("Snapshot changing diffIndex from=%d to=%d me=%d logLen=%d\n", rf.log[0].Index, index, rf.me, len(rf.log))
	rf.log = rf.log[index-rf.log[0].Index:]
	rf.log[0].Index = index
}
