package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type State int

const (
	Idle State = iota
	MapTask
	ReduceTask
	Kill
)

const NReduce int = 11

type WorkerStruct struct {
	id           int // Capitalize maybe
	state        State
	mapFile      string
	mapFileIndex int
	reduceHash   int
	WorkerLock   sync.Mutex
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	worker := WorkerStruct{id: -1, state: Idle}

	// Get ID for worker
	ch := make(chan bool, 1)
	reply := WorkerReply{}
	go func() {
		reply = GetId(&worker)
		ch <- true
		defer close(ch)
	}()
	select {
	case <-ch:
		worker.WorkerLock.Lock()
		worker.id = reply.Id
		worker.WorkerLock.Unlock()
	case <-time.After(time.Second * 3):
		fmt.Println("W: Could not reach master")
	}
	fmt.Printf("W[%d] received id\n", worker.id)

	// Worker tasks:
	// Thread 1: Process map/reduce task
	// Thread 2: Call master or master calls worker to know status.
	//     - Request master for task
	//     - Master may not respond with any info. If so, exit after a period.
	//     - If a Reduce task, be careful with data read from map?

	for {
		worker.WorkerLock.Lock()
		state := worker.state
		worker.WorkerLock.Unlock()
		if state == Idle {
			// Worker sends a request to get task. Waits for 3 seconds before retrying
			quit := make(chan bool, 1)
			go func() {
				reply = RequestTask(&worker) // goroutine and wait
				quit <- true
				defer close(quit)
			}()
			select {
			case <-quit:
				if reply.Id != worker.id {
					log.Fatalln("Coordinator returned to worker ", worker.id, " but was for worker ", reply.Id)
				}
				worker.WorkerLock.Lock()
				worker.state = reply.NewState
				if worker.state == Kill {
					fmt.Printf("W[%d] told by coordinator to die\n", worker.id)
					worker.WorkerLock.Unlock()
					continue
				} else if worker.state == MapTask {
					worker.mapFile = reply.Filename
					worker.mapFileIndex = reply.FilenameIndex
				} else if worker.state == ReduceTask {
					worker.reduceHash = reply.HashId
				} else if worker.state == Idle {
					// Probably a phase is done. For now just kill
					fmt.Printf("W[%d] assigned to IDLE for 2 seconds\n", worker.id)
					time.Sleep(2 * time.Second)
					worker.state = Idle
				}
				worker.WorkerLock.Unlock()

			case <-time.After(time.Second * 3):
				fmt.Println("Could not get task for ", worker.id, " Retrying. State should be idle: ", worker.state)
				if worker.state != Idle {
					panic("Idle changed")
				}
				worker.WorkerLock.Lock()
				worker.state = Kill
				worker.WorkerLock.Unlock()
				continue
			}

		} else if state == MapTask {
			runMap(&worker, mapf)

			quit := make(chan bool, 1)
			reply := DoneMapReply{}
			go func() {
				reply = MarkMapDone(&worker)
				quit <- true
				defer close(quit)
			}()
			select {
			case <-quit:
				fmt.Printf("W[%d] informed coordinator successfully of completed map with reply %t\n", reply.Id, reply.Recorded)
				worker.WorkerLock.Lock()
				worker.state = Idle
				worker.WorkerLock.Unlock()
			case <-time.After(time.Second * 10):
				// fmt.Print("Could not inform coordinator", worker.id)
				worker.WorkerLock.Lock()
				worker.state = Kill // Coordinator is assumed to be dead
				worker.WorkerLock.Unlock()
			}

		} else if state == ReduceTask {
			runReduce(&worker, reducef)
			quit := make(chan bool, 1)
			reply := DoneReduceReply{}
			go func() {
				reply = MarkReduceDone(&worker)
				quit <- true
				defer close(quit)
			}()
			select {
			case <-quit:
				fmt.Printf("W[%d] informed coordinator successfully of completed REDUCED with reply %t\n", reply.Id, reply.Recorded)
				worker.WorkerLock.Lock()
				worker.state = Idle
				worker.WorkerLock.Unlock()
			case <-time.After(time.Second * 10):
				// fmt.Print("Could not inform coordinator", worker.id)
				worker.WorkerLock.Lock()
				worker.state = Kill // Coordinator is assumed to be dead
				worker.WorkerLock.Unlock()
			}
		} else if state == Kill {
			fmt.Printf("W[%d] died\n", worker.id)
			break
		}
	}

}

func runMap(worker *WorkerStruct, mapf func(string, string) []KeyValue) {
	fmt.Printf("W[%d] running MAP on %s\n", worker.id, worker.mapFile)

	filename := worker.mapFile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content)) // Array of KeyValue type
	buckets := make(map[int]([]KeyValue))
	for _, pair := range kva {
		partition := ihash(pair.Key) % NReduce
		if buckets[partition] == nil {
			buckets[partition] = []KeyValue{}
		}

		buckets[partition] = append(buckets[partition], pair)
	}
	fmt.Printf("W[%d] built MAP buckets for %s\n", worker.id, worker.mapFile)

	// Parallelize this for multiple writes
	for i := 0; i < NReduce; i++ {
		pName := fmt.Sprintf("mr-%d-%d", worker.mapFileIndex, i)
		tmpfile, err := ioutil.TempFile(".", pName)
		if err != nil {
			log.Fatal(err)
		}
		enc := json.NewEncoder(tmpfile)
		for _, kv := range buckets[i] {
			err := enc.Encode(&kv)

			if err != nil {
				log.Fatalf("Could not save partition")
			}

		}
		err = os.Rename(tmpfile.Name(), pName)
		if err != nil {
			log.Fatal(err)
		}
	}

}

func runReduce(worker *WorkerStruct, reducef func(string, []string) string) {
	worker.WorkerLock.Lock()
	workerId := worker.id
	key := worker.reduceHash
	worker.WorkerLock.Unlock()
	fmt.Printf("W[%d] running REDUCE on hash %d\n", worker.id, key)
	// Collect all files with fixed assigned hash.
	// Read them in and sort them
	// Output results to one file called mr-out-workerId
	intermediate := []KeyValue{}                             // Array of KeyValue type
	files, err := filepath.Glob(fmt.Sprintf("mr-*-%d", key)) // Does not handle >1 digit workers
	if err != nil {
		// handle errors
	}
	fmt.Printf("W[%d] using partitions %s\n", workerId, files)
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("W[", workerId, "] could not open the partitions!")
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))
	pName := fmt.Sprintf("mr-out-%d", key)
	tmpfile, err := ioutil.TempFile(".", pName)
	if err != nil {
		log.Fatal(err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	err = os.Rename(tmpfile.Name(), pName)
	fmt.Printf("W[%d] wrote to output file %s\n", workerId, pName)
	if err != nil {
		log.Fatal(err)
	}
}

func GetId(worker *WorkerStruct) WorkerReply {
	args := WorkerArgs{}
	reply := WorkerReply{}
	ok := call("Coordinator.GetId", &args, &reply)
	if !ok {
		fmt.Println("W: Could not get receive ID")
	}
	return reply
}

func RequestTask(worker *WorkerStruct) WorkerReply {
	worker.WorkerLock.Lock()
	args := WorkerArgs{State: worker.state, Id: worker.id}
	reply := WorkerReply{}
	worker.WorkerLock.Unlock()
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		fmt.Println("W: Assignment error")
	}
	return reply
}

func MarkMapDone(worker *WorkerStruct) DoneMapReply { // Informs coordinator of completed map task
	worker.WorkerLock.Lock()
	workerId := worker.id
	filename := worker.mapFile
	worker.WorkerLock.Unlock()
	partitionFiles := make([]string, NReduce)
	for i := 0; i < NReduce; i++ {
		partitionFiles[i] = fmt.Sprintf("mr-%d-%d", workerId, i)
	}
	fmt.Printf("W[%d] MAP informing completion of %s\n", workerId, filename)
	fmt.Printf("W[%d] MAP with partitions %s\n", workerId, partitionFiles)
	args := DoneMapArgs{Id: workerId, Filename: filename, PartitionFiles: partitionFiles}
	reply := DoneMapReply{}
	ok := call("Coordinator.MarkMapDone", &args, &reply)
	if !ok {
		log.Fatalf("W[%d] could not inform coordinator of completion", workerId)
	}
	return reply
}

func MarkReduceDone(worker *WorkerStruct) DoneReduceReply {
	worker.WorkerLock.Lock()
	workerId := worker.id
	hashId := worker.reduceHash
	worker.WorkerLock.Unlock()
	fmt.Printf("W[%d] sending completed hash %d\n", workerId, hashId)
	args := DoneReduceArgs{Id: workerId, HashId: hashId}
	reply := DoneReduceReply{}

	ok := call("Coordinator.MarkReduceDone", &args, &reply)
	if !ok {
		log.Fatalf("Could not inform coordinator of completed reduce")
	}
	return reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
