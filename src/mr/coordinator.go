package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	Finished
)

type Coordinator struct {
	files                []string
	filesToIndices       map[string]int
	mapsCompleted        map[string]int
	mapsRunning          map[string](map[int]bool)
	mapsTimeAssigned     map[string]time.Time
	reducesCompleted     map[int]int
	reducesRunning       map[int](map[int]bool)
	reducesTimeAssigned  map[int]time.Time
	phase                Phase
	nextWorkerId         int
	mapsCompletedLock    sync.Mutex
	reducesCompletedLock sync.Mutex
	mapsRunningLock      sync.Mutex
	reducesRunningLock   sync.Mutex
	workerCountLock      sync.Mutex
	phaseLock            sync.Mutex
	nreduce              int
}

type ReduceFile struct {
	HashId int
}

func (c *Coordinator) GetId(args *WorkerArgs, reply *WorkerReply) error {
	c.workerCountLock.Lock()
	reply.Id = c.nextWorkerId
	c.nextWorkerId += 1
	fmt.Printf("W[%d] given ID\n", reply.Id)
	c.workerCountLock.Unlock()
	return nil
}

func (c *Coordinator) GetTask(args *WorkerArgs, reply *WorkerReply) error {
	workerId := args.Id
	reply.Id = workerId
	c.phaseLock.Lock()
	phase := c.phase
	c.phaseLock.Unlock()

	if phase == MapPhase {
		completedMapPhase := true
		c.mapsCompletedLock.Lock() // LOCK MAP COMPLETE

		for file, completion := range c.mapsCompleted {
			if completion == 0 {
				completedMapPhase = false

				c.mapsRunningLock.Lock() // LOCK MAP RUNNING
				if c.mapsRunning[file] == nil {
					c.mapsRunning[file] = make(map[int]bool)
				}

				elapsed := time.Now().Sub(c.mapsTimeAssigned[file]).Seconds()
				if elapsed > 10 {
					c.mapsTimeAssigned[file] = time.Now()
					c.mapsRunning[file][workerId] = true

					fmt.Printf("W[%d] assigned to MAP task %s\n", workerId, file)
					fmt.Printf("W[%d] assigned to MAP task %s after elapsed time %f\n", workerId, file, elapsed)
					fmt.Printf("W[%d] updated MAP timings %s\n", workerId, c.mapsTimeAssigned[file])
					reply.NewState = MapTask
					reply.Filename = file
					reply.FilenameIndex = c.filesToIndices[file]
					c.mapsRunningLock.Unlock() // UNLOCK MAP RUNNING
					c.mapsCompletedLock.Unlock()
					return nil
					// Time has elapsed too much
				} else {
					fmt.Printf("W[%d] MAP task %s NOT READY %f\n", workerId, file, elapsed)
					c.mapsRunningLock.Unlock() // UNLOCK MAP RUNNING
					continue
				}
			} else {
				// fmt.Println("Going to next file for worker ", workerId)
			}
		}
		c.mapsCompletedLock.Unlock() // UNLOCK MAP COMPLETE
		fmt.Printf("W[%d] no available MAP tasks to try\n", workerId)
		reply.NewState = Idle

		if completedMapPhase { // Benign race? Multiple workers may be here when map phase completes
			fmt.Printf("W[%d] attempting to transition to REDUCE PHASE\n", workerId)
			c.phaseLock.Lock()
			if c.phase == MapPhase {
				c.phase = ReducePhase // If c.phase == Finished, then this will revert it back to reduce without locking
				fmt.Printf("C: ENTERING REDUCE PHASE\n")
			}
			c.phaseLock.Unlock()
		} else {
			fmt.Printf("W[%d] MAP tasks remaining but nothing to assign\n", workerId)
		}
	} else if phase == ReducePhase {
		completedReducePhase := true
		fmt.Printf("W[%d] entered REDUCE phase\n", workerId)

		c.reducesCompletedLock.Lock()

		for hashId, completion := range c.reducesCompleted {
			if completion == 0 {
				completedReducePhase = false
				// fmt.Println("Coordinator examing file ", file, " with completion ", completion)

				c.reducesRunningLock.Lock() // LOCK  REDUCE RUNNING
				if c.reducesRunning[hashId] == nil {
					c.reducesRunning[hashId] = make(map[int]bool)
				}

				// for _, workerList := range c.reducesRunning { // Checks if worker already running task
				// 	if _, ok := workerList[workerId]; ok {
				// 		fmt.Println("Worker ", workerId, " already assigned to reduce task")
				// 	}
				// }

				if len(c.reducesRunning[hashId]) > 0 { // Currently policy is to assign one worker per task
					// fmt.Println("Worker ", workerId, " encountered workers ", c.reducesRunning[file], " with same reduces task")
					c.reducesRunningLock.Unlock() // UNLOCK RELEASE RUNNING
					continue
				}
				c.reducesRunning[hashId][workerId] = true

				fmt.Printf("W[%d] assigned to REDUCE hash %d\n", workerId, hashId)
				reply.NewState = ReduceTask
				reply.HashId = hashId
				c.reducesRunningLock.Unlock() // UNLOCK RELEASE RUNNING
				c.reducesCompletedLock.Unlock()
				return nil
			} else {
				// fmt.Println("Going to next file for worker ", workerId)
			}
		}
		c.reducesCompletedLock.Unlock()
		fmt.Printf("W[%d] no available REDUCE task to try\n", workerId)
		reply.NewState = Idle

		if completedReducePhase { // Benign race? Multiple workers may be here when map phase completes
			fmt.Printf("W[%d] attempting to transition to FINISHED PHASE\n", workerId)
			c.phaseLock.Lock()
			if c.phase == ReducePhase {
				c.phase = Finished // If c.phase == Finished, then this will revert it back to reduce without locking
				fmt.Printf("C: ENTERING FINISHED PHASE\n")
			} else if c.phase == MapPhase {
				log.Fatalf("W[%d] not supposed to be in map phase", workerId)
			}
			c.phaseLock.Unlock()
		} else {
			fmt.Println("More reduce tasks remaining but nothing to assign yet")
		}
	} else if phase == Finished {
		fmt.Printf("W[%d] entered Finished phase\n", workerId)
		reply.NewState = Kill
	}

	return nil
}

func (c *Coordinator) MarkMapDone(args *DoneMapArgs, reply *DoneMapReply) error {
	workerId := args.Id
	filename := args.Filename
	// TODO: assert that this work was actually worker
	c.mapsCompletedLock.Lock()
	if c.mapsCompleted[filename] != 0 { // Was previously completed. Ignore this worker
		fmt.Println("Repeated map work: ", filename, " by worker ", workerId)
		reply.Id = workerId
		reply.Recorded = true
		c.mapsCompletedLock.Unlock()
		return nil
	}
	c.mapsRunningLock.Lock()
	if workers, ok := c.mapsRunning[filename]; ok {
		// Remove filename from running map tasks (and associated workers)
		// -If not completed yet, set as true and save file names
		// -Else ignore file names
		if _, ok := workers[workerId]; !ok { // Verify that worker did this task
			log.Fatalf("W[%d] not responsible for completing MAP task %s\n", workerId, filename)
		}

		// First time completed
		c.mapsCompleted[filename] = workerId
		// partitionFiles := args.PartitionFiles
		fmt.Printf("W[%d] confirmation of done MAP task %s\n", workerId, filename)

		// fmt.Println("Before running for marked worker ", workerId, " ", c.mapsRunning)
		delete(c.mapsRunning, filename) // WARNING: NOP
		// fmt.Println("Update running for marked worker ", workerId, " ", c.mapsRunning)
		// fmt.Println("Updated completed map for marked worker ", workerId, " ", c.mapsCompleted)
		reply.Recorded = true
		reply.Id = workerId

	} else {
		log.Fatalf("W[%d] NOT assigned to this MAP %s\n", workerId, filename)
		// Map task never existed. Should be impossible with good code.
		// TODO: Flag this
	}
	c.mapsRunningLock.Unlock()
	c.mapsCompletedLock.Unlock()
	return nil

}

func (c *Coordinator) MarkReduceDone(args *DoneReduceArgs, reply *DoneReduceReply) error {
	workerId := args.Id
	hashId := args.HashId

	c.reducesCompletedLock.Lock()
	if c.reducesCompleted[hashId] != 0 { // Was previously completed. Ignore this worker
		fmt.Printf("W[%d] repeated REDUCE hash %d\n", hashId, workerId)
		reply.Id = workerId
		reply.Recorded = true
		c.reducesCompletedLock.Unlock()
		return nil
	}
	c.reducesRunningLock.Lock()
	if workers, ok := c.reducesRunning[hashId]; ok {
		// Remove filename from running reduce tasks (and associated workers)
		// -If not completed yet, set as true and save file names
		if _, ok := workers[workerId]; !ok { // Verify that worker did this task
			log.Fatalf("W[%d] not responsible for REDUCE task hash %d\n", workerId, hashId)
		}

		// First time completed
		c.reducesCompleted[hashId] = workerId
		fmt.Printf("W[%d] confirmation of REDUCED hash %d\n", workerId, hashId)

		// fmt.Println("Before running for marked reduces worker ", workerId, " ", c.reducesRunning)
		delete(c.reducesRunning, hashId) // WARNING: NOP
		// fmt.Println("Update running for marked reduce worker ", workerId, " ", c.reducesRunning)
		// fmt.Println("Updated completed map for marked reduce worker ", workerId, " ", c.reducesCompleted)
		reply.Recorded = true
		reply.Id = workerId

	} else {
		log.Fatal("Unassigned worker ", workerId, " marked reduce task as done: ")
		// Map task never existed. Should be impossible with good code.
	}
	c.reducesRunningLock.Unlock()
	c.reducesCompletedLock.Unlock()
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
	// c.phaseLock.Lock()
	// ret = c.phase == Finished
	// c.phaseLock.Unlock()

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
	c.filesToIndices = make(map[string]int)
	c.mapsCompleted = make(map[string]int)
	c.reducesCompleted = make(map[int]int)
	c.mapsRunning = make(map[string](map[int]bool))
	c.reducesRunning = make(map[int]map[int]bool)
	c.mapsTimeAssigned = make(map[string]time.Time)
	c.reducesTimeAssigned = make(map[int]time.Time)
	c.nextWorkerId = 1
	c.nreduce = 11

	for i, file := range files {
		c.mapsCompleted[file] = 0
		c.filesToIndices[file] = i
		c.mapsTimeAssigned[file] = time.Time{}
	}

	for i := 0; i < c.nreduce; i++ {
		c.reducesCompleted[i] = 0
		c.reducesTimeAssigned[i] = time.Time{}
	}
	c.server()
	return &c
}
