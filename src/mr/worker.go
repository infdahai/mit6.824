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
	"time"
)

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

	for {
		task := AskForTask()
		switch task.TaskState {
		case Maper:
			Map(&task, mapf)
		case Reducer:
			Reduce(&task, reducef)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			return
		}
	}

}

func AskForTask() Task {
	args := ExampleArgs{}
	reply := Task{}
	call("Coordinator.DistriTask", &args, &reply)
	return reply
}

func TaskCompleted(task *Task) {
	reply := ExampleReply{}
	call("Coordinator.TaskCompleted", task, &reply)
}

func Map(task *Task, mapf func(string, string) []KeyValue) {
	content, err := ioutil.ReadFile(task.Input)
	if err != nil {
		log.Fatalf("cannot read %s: %v", task.Input, err)
	}
	intermediates := mapf(task.Input, string(content))
	buf := make([][]KeyValue, task.NReduce)
	for _, intermediate := range intermediates {
		ind := ihash(intermediate.Key) % task.NReduce
		buf[ind] = append(buf[ind], intermediate)
	}
	outPut := make([]string, 0)
	for i := 0; i < task.NReduce; i++ {
		outPut = append(outPut, writeLocalFile(task.TaskNum, i, &buf[i]))
	}
	task.Intermediate = outPut
	TaskCompleted(task)
}

func writeLocalFile(x int, y int, kva *[]KeyValue) string {
	dir, _ := os.Getwd()
	tempF, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatalf("cannot create temporary file: %v", err)
	}
	enc := json.NewEncoder(tempF)
	for _, kv := range *kva {
		if err := enc.Encode(&kv); err != nil {
			log.Fatalf("cannot encode kva key: %v", err)
		}
	}
	tempF.Close()
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(tempF.Name(), outputName)
	return filepath.Join(dir, outputName)
}

func Reduce(task *Task, reducef func(string, []string) string) {
	intermediate := *readLocalFile(task.Intermediate)
	sort.Sort(ByKey(intermediate))
	dir, _ := os.Getwd()
	tempF, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatalf("cannot create temporary file: %v", err)
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
		fmt.Fprintf(tempF, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempF.Close()
	oname := fmt.Sprintf("mr-out-%d", task.TaskNum)
	os.Rename(tempF.Name(), oname)
	task.Output = oname
	TaskCompleted(task)
}

func readLocalFile(files []string) *[]KeyValue {
	kva := []KeyValue{}
	for _, filepath := range files {
		file, err := os.Open(filepath)
		if err != nil {
			log.Fatalf("cannot open file %s: %v", filepath, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return &kva
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
