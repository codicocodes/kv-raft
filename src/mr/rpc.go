package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// example to show how to declare the arguments
// and reply for an RPC.

type GetNextTaskArgs struct {
	ID int
	Command CommandType
}

type CommandType int

func (t CommandType) String() string {
	switch t {
	case 0:
		return "Map"
	case 1:
		return "Reduce"
	case 2:
		return "Wait"
	case 3:
		return "Completed"
	}
	panic("unexpected tasktype received")
}

const (
	MapCommand    CommandType = iota
	ReduceCommand
	WaitCommand
	CompletedCommand
)

type GetNextTaskReply struct {
	Type       CommandType 
	MapTask    *MapTask
	ReduceTask *ReduceTask
	Partitions int
	ID         int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
