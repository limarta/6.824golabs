package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type WorkerArgs struct {
	State State
	Id    int
}

type WorkerReply struct {
	NewState   State
	Filename   string
	Id         int
	Partitions int
}

type DoneSignalArgs struct {
	Id             int
	Filename       string
	Task           State
	PartitionFiles []string
}

type DoneSignalReply struct {
	Id       int
	Recorded bool
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
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
