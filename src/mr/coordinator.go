package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	Finished
)

type Coordinator struct {
	files                []string
	mapsCompleted        map[string]int
	reducesCompleted     map[string]bool
	mapsRunning          map[string](map[int]bool)
	reducesRunning       map[int]int
	phase                Phase
	nextWorkerId         int
	mapsCompletedLock    sync.Mutex
	reducesCompletedLock sync.Mutex
	mapsRunningLock      sync.Mutex
	reducesrunningLock   sync.Mutex
	workerCountLock      sync.Mutex
}

type ReduceFile struct {
	HashId int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetId(args *WorkerArgs, reply *WorkerReply) error {
	c.workerCountLock.Lock()
	reply.Id = c.nextWorkerId
	c.nextWorkerId += 1
	c.workerCountLock.Unlock()
	return nil
}

func (c *Coordinator) GetTask(args *WorkerArgs, reply *WorkerReply) error {
	workerId := args.Id

	if c.phase == MapPhase {
		// fmt.Println("Coordinator searching a map task for worker")

		c.mapsCompletedLock.Lock() // LOCK MAP COMPLETE
		// fmt.Println(c.mapsCompleted)

		for file, completion := range c.mapsCompleted {
			if completion == 0 {
				// fmt.Println("Coordinator examing file ", file, " with completion ", completion)

				c.mapsRunningLock.Lock() // LOCK MAP RUNNING
				if c.mapsRunning[file] == nil {
					c.mapsRunning[file] = make(map[int]bool)
				}

				// TODO: Check that worker doesn't already have a task
				if _, ok := c.mapsRunning[file][workerId]; ok {
					log.Fatal("This worker is doing this task: ", c.mapsRunning[file])
				}
				if len(c.mapsRunning[file]) > 1 {
					fmt.Println("Encountered a worker with same map task: ", c.mapsRunning[file])
				}
				c.mapsRunning[file][workerId] = true
				c.mapsRunningLock.Unlock() // UNLOCK MAP RUNNING
				reply.NewState = MapTask
				reply.Filename = file
				reply.Id = workerId
				c.mapsCompletedLock.Unlock()
				return nil
			}
		}
		c.mapsCompletedLock.Unlock() // UNLOCK MAP COMPLETE
	} else if c.phase == ReducePhase {

	}

	return nil
}

func (c *Coordinator) MarkMapDone(args *DoneSignalArgs, reply *DoneSignalReply) error {
	workerId := args.Id
	filename := args.Filename

	// TODO: assert that this work was actually worker
	c.mapsCompletedLock.Lock()
	c.mapsRunningLock.Lock()
	if workers, ok := c.mapsRunning[filename]; ok {
		// Remove filename from running map tasks (and associated workers)
		// -If not completed yet, set as true and save file names
		// -Else ignore file names
		if _, ok := workers[workerId]; !ok { // Verify that worker did this task
			log.Fatal("Worker was not responsible for completing this task")
		}

		if c.mapsCompleted[filename] != 0 {
			// Was previously completed
		} else {
			// First time completed
			c.mapsCompleted[filename] = workerId
			partitionFiles := args.PartitionFiles
			fmt.Println("Coordinator received partitions ", partitionFiles)
		}

		fmt.Println("Before running for marked worker ", workerId, " ", c.mapsRunning)
		delete(c.mapsRunning, filename) // WARNING: NOP
		fmt.Println("Update running for marked worker ", workerId, " ", c.mapsRunning)
		fmt.Println("Updated completed map for marked worker ", workerId, " ", c.mapsCompleted)
		reply.Recorded = true
		reply.Id = workerId

	} else {
		log.Fatal("Unassigned worker ", workerId, " marked map task as done: ")
		// Map task never existed. Should be impossible with good code.
		// TODO: Flag this
	}
	c.mapsRunningLock.Unlock()
	c.mapsCompletedLock.Unlock()
	return nil

}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil) // Change this here to handle worker
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.phase = MapPhase
	c.files = files
	c.mapsCompleted = make(map[string]int)
	c.reducesCompleted = make(map[string]bool)
	c.mapsRunning = make(map[string](map[int]bool))
	c.reducesRunning = make(map[int]int)
	c.nextWorkerId = 1

	for _, file := range files {
		c.mapsCompleted[file] = 0
		c.reducesCompleted[file] = false
	}
	c.server()
	return &c
}
