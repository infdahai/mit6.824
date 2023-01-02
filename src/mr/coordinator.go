package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type CoorPhase int

const (
	Maper CoorPhase = iota
	Reducer
	Exit
	Wait
)

type MetaTaskState int

const (
	Idle MetaTaskState = iota
	Progressing
	Completed
)

type MetaTask struct {
	TaskStatus MetaTaskState
	StartTime  time.Time
	TaskRef    *Task
}

type Coordinator struct {
	TaskQueue    chan *Task
	TaskMeta     map[int]*MetaTask
	Phase        CoorPhase
	NReduce      int
	Input        []string
	Intermediate [][]string
}

type Task struct {
	Input        string
	TaskState    CoorPhase
	NReduce      int
	TaskNum      int
	Intermediate []string
	Output       string
}

var mu sync.Mutex

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
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	ret := c.Phase == Exit
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskQueue:    make(chan *Task, max(nReduce, len(files))),
		TaskMeta:     make(map[int]*MetaTask),
		Phase:        Maper,
		NReduce:      nReduce,
		Input:        files,
		Intermediate: make([][]string, nReduce),
	}
	c.makeMapTask()
	c.server()
	go c.catchTimeOut()
	return &c
}

func (c *Coordinator) makeMapTask() {
	for ind, filename := range c.Input {
		task := Task{
			Input:     filename,
			TaskState: Maper,
			NReduce:   c.NReduce,
			TaskNum:   ind,
		}
		c.TaskQueue <- &task
		c.TaskMeta[ind] = &MetaTask{
			TaskStatus: Idle,
			TaskRef:    &task,
		}
	}
}

func (c *Coordinator) makeReduceTask() {
	c.TaskMeta = make(map[int]*MetaTask)
	for ind, files := range c.Intermediate {
		task := Task{
			TaskState:    Reducer,
			NReduce:      c.NReduce,
			TaskNum:      ind,
			Intermediate: files,
		}
		c.TaskQueue <- &task
		c.TaskMeta[ind] = &MetaTask{
			TaskStatus: Idle,
			TaskRef:    &task,
		}
	}
}

func (c *Coordinator) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		if c.Phase == Exit {
			mu.Unlock()
			return
		}
		for _, metaTask := range c.TaskMeta {
			if metaTask.TaskStatus == Progressing && time.Since(metaTask.StartTime) > 10*time.Second {
				c.TaskQueue <- metaTask.TaskRef
				metaTask.TaskStatus = Idle
			}
		}
		mu.Unlock()
	}
}

func (c *Coordinator) DistriTask(args *ExampleArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	if len(c.TaskQueue) > 0 {
		*reply = *<-c.TaskQueue
		c.TaskMeta[reply.TaskNum].TaskStatus = Progressing
		c.TaskMeta[reply.TaskNum].StartTime = time.Now()
	} else if c.Phase == Exit {
		*reply = Task{
			TaskState: Exit,
		}
	} else {
		*reply = Task{
			TaskState: Wait,
		}
	}
	return nil
}

func (c *Coordinator) TaskCompleted(task *Task, reply *ExampleReply) error {
	mu.Lock()
	if task.TaskState != c.Phase || c.TaskMeta[task.TaskNum].TaskStatus == Completed {
		return nil
	}
	c.TaskMeta[task.TaskNum].TaskStatus = Completed
	mu.Unlock()
	defer c.processTask(task)
	return nil
}

func (c *Coordinator) processTask(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskState {
	case Maper:
		for reduceId, filePath := range task.Intermediate {
			c.Intermediate[reduceId] = append(c.Intermediate[reduceId], filePath)
		}
		if c.finishAllTask() {
			c.makeReduceTask()
			c.Phase = Reducer
		}
	case Reducer:
		if c.finishAllTask() {
			c.Phase = Exit
		}
	}
}

func (c *Coordinator) finishAllTask() bool {
	for _, taskMeta := range c.TaskMeta {
		if taskMeta.TaskStatus != Completed {
			return false
		}
	}
	return true
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
