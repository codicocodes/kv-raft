package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func readFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

const (
	TMP_FILE_NAME = "tmp-%d-%d"
	TMP_PREFIX    = "tmp"
	FINAL_PREFIX  = "mr"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func mapHandler(mapf func(string, string) []KeyValue, reply GetNextTaskReply) {
	task := reply.MapTask
	fileContent := readFile(task.File)
	kva := mapf(task.File, fileContent)
	sort.Sort(ByKey(kva))
	openFiles := make(map[string]*os.File)
	for _, kv := range kva {
		tmpFilename := fmt.Sprintf(TMP_FILE_NAME, task.FileID, ihash(kv.Key)%reply.Partitions)
		file, ok := openFiles[tmpFilename]
		if !ok {
			openedFile, err := os.OpenFile(tmpFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				panic(errors.New("[MAP]" + err.Error()))
			}
			file = openedFile
			openFiles[tmpFilename] = file
		}
		enc := json.NewEncoder(file)
		err := enc.Encode(&kv)
		if err != nil {
			panic(errors.New("[MAP]" + err.Error()))
		}
	}
	// close our open files
	// rename the files atomically
	for tmpFilename, file := range openFiles {
		file.Close()
		filename := strings.Replace(tmpFilename, TMP_PREFIX, FINAL_PREFIX, 1)
		os.Rename(tmpFilename, filename)
	}
}

func reduceHandler(reducef func(string, []string) string, reply GetNextTaskReply) {
	var intermediate []KeyValue
	for i := 0; i <= reply.ReduceTask.FileCount; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reply.ReduceTask.PartitionID)
		file, err := os.Open(filename)
		if err != nil {
			continue
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
	tmpname := fmt.Sprintf("tmp-out-%d", reply.ReduceTask.PartitionID)
	oname := fmt.Sprintf("mr-out-%d", reply.ReduceTask.PartitionID)
	tmpfile, err := os.Create(tmpname)
	if err != nil {
		panic("[REDUCE] " + err.Error())
	}
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
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
	tmpfile.Close()
	os.Rename(tmpname, oname)
	// fmt.Printf("Finnished creating %s\n", oname)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	var taskID = -1
	var command = WaitCommand
	for {
		reply := CallGetNextTask(taskID, command)
		command = reply.Type
		taskID = reply.ID
		switch command {
		case WaitCommand:
		case CompletedCommand:
			os.Exit(0)
		case MapCommand:
			mapHandler(mapf, reply)
		case ReduceCommand:
			reduceHandler(reducef, reply)
		}
	}
}

// Make an RPC call to the coordinator to receive the next file to work on
// the RPC argument and reply types are defined in rpc.go.
func CallGetNextTask(taskID int, command CommandType) GetNextTaskReply {
	args := GetNextTaskArgs{
		ID:      taskID,
		Command: command,
	}
	reply := GetNextTaskReply{}
	if ok := call("Coordinator.GetNextTask", &args, &reply); ok {
		return reply
	}
	panic("Call to get next task failed!\n")
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// log.Fatal("dialing:", err)
		os.Exit(1)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	return err == nil
}
