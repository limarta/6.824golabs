package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

type State int

const (
	Idle State = iota
	MapTask
	ReduceTask
	Kill
)

const NReduce int = 10

type WorkerStruct struct {
	id      int // Capitalize maybe
	state   State
	mapFile string
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
	defer close(ch)
	go func() {
		GetId(&worker)
		ch <- true
	}()
	select {
	case <-ch:
		fmt.Println("Earned id ", worker.id)
	case <-time.After(time.Second * 3):
		fmt.Println("Could not reach master")
	}

	// Worker tasks:
	// Thread 1: Process map/reduce task
	// Thread 2: Call master or master calls worker to know status.
	//     - Request master for task
	//     - Master may not respond with any info. If so, exit after a period.
	//     - If a Reduce task, be careful with data read from map?

	for {
		if worker.state == Idle {
			// Worker sends a request to get task. Waits for 3 seconds before retrying
			quit := make(chan bool, 1)
			func() {
				RequestTask(&worker) // goroutine and wait
				quit <- true
			}()
			select {
			case <-quit:
			case <-time.After(time.Second * 3):
				fmt.Print("Could not get task. Retrying. State should be idle: ", worker.state)
			}

			fmt.Println("Worker ", worker.id, " received map file ", worker.mapFile)

		}
		if worker.state == MapTask {
			runMap(&worker, mapf)

			quit := make(chan bool, 1)
			func() {
				MarkMapDone(&worker)
				quit <- true
			}()
			select {
			case <-quit:
				fmt.Println("Informed coordinator successfully of completed map")
			case <-time.After(time.Second * 10):
				fmt.Print("Could not inform coordinator", worker.id)
			}
			worker.state = Kill

		} else if worker.state == ReduceTask {
		} else if worker.state == Kill {
			fmt.Println("Worker ", worker.id, " killed!")
			break
		}
	}

}

func runMap(worker *WorkerStruct, mapf func(string, string) []KeyValue) {
	fmt.Println("Running map for ", worker.id)

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
	fmt.Println("Built buckets for worker ", worker.id)

	for i := 0; i < NReduce; i++ {
		pName := fmt.Sprintf("mr-%d-%d", worker.id, i)
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

func GetId(worker *WorkerStruct) WorkerReply {
	args := WorkerArgs{}
	reply := WorkerReply{}
	ok := call("Coordinator.GetId", &args, &reply)
	if ok {
		worker.id = reply.Id
	} else {
		fmt.Println("Could not get receive ID")
	}
	return reply
}

func RequestTask(worker *WorkerStruct) WorkerReply {
	args := WorkerArgs{State: worker.state, Id: worker.id}
	reply := WorkerReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		if reply.Id != worker.id {
			log.Fatalln("Coordinator returned to wrong worker")
		}
		worker.state = reply.NewState

		if worker.state == Kill {
			// Kill this worker
		} else if worker.state == MapTask {
			worker.mapFile = reply.Filename
		} else if worker.state == ReduceTask {
			worker.state = reply.NewState
		}
		worker.state = reply.NewState
	} else {
		fmt.Println("Assignment error")
	}
	return reply
}

func MarkMapDone(worker *WorkerStruct) { // Informs coordinator of completed map task
	partitionFiles := make([]string, NReduce)
	for i := 0; i < NReduce; i++ {
		partitionFiles[i] = fmt.Sprintf("mr-%d-%d", worker.id, i)
	}
	fmt.Println("Sending partitions ", partitionFiles)
	args := DoneSignalArgs{Id: worker.id, Filename: worker.mapFile, PartitionFiles: partitionFiles}
	reply := DoneSignalReply{}
	ok := call("Coordinator.MarkMapDone", &args, &reply)
	if ok {

	} else {
		log.Fatalf("Could not inform coordinator of completion")
	}
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
