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

type TaskStatus int

const (
	Pending TaskStatus = iota
	Active
	Completed
)

type Task struct {
	ID        int
	Type      CommandType
	StartedAt time.Time
}

type MapTask struct {
	Task
	File   string
	FileID int
}

type ReduceTask struct {
	Task
	PartitionID int
	FileCount   int
}

type Coordinator struct {
	sync.Mutex
	mapQueue    []MapTask
	mapTasks    map[int]MapTask
	reduceQueue []ReduceTask
	reduceTasks map[int]ReduceTask
	mapped      bool
	partitions  int
	id          int
}

// TODO:
// ✅ can actually pop from here
// ✅ then we will store the ongoing task in the mapTasks map
// ✅ the task successfully returns, we will delete it from the mapTasks map
// ✅ will check that the queue is empty and that the tasks is empty
// ✅ save completed state on the coordinator
// ✅ the reduce handler
// ✅ if worker task is dead
// ✅ we need to resend a task, we can remove it from the tasksMap
// ✅ add it back to the queue
// ✅ Done func
// Reduce Parallelism test is failing
// Crash test is failing

func (c *Coordinator) getMapTask() *MapTask {
	c.Lock()
	defer c.Unlock()
	if len(c.mapQueue) == 0 {
		return nil
	}
	task, rest := c.mapQueue[0], c.mapQueue[1:]
	c.mapQueue = rest
	c.mapTasks[task.ID] = task
	go func() {
		time.Sleep(10 * time.Second)
		c.Lock()
		defer c.Unlock()
		if _, ok := c.mapTasks[task.ID]; ok {
			c.id++
			newTask := MapTask{
				Task: Task{
					ID:        c.id,
					StartedAt: time.Now(),
				},
				File:   task.File,
				FileID: task.FileID,
			}
			c.mapQueue = append(c.mapQueue, newTask)
			delete(c.mapTasks, task.ID)
		}
	}()
	return &task
}

func (c *Coordinator) getReduceTask() *ReduceTask {
	c.Lock()
	defer c.Unlock()
	if len(c.reduceQueue) == 0 {
		return nil
	}
	task, rest := c.reduceQueue[0], c.reduceQueue[1:]
	c.reduceQueue = rest
	c.reduceTasks[task.ID] = task
	go func() {
		time.Sleep(10 * time.Second)
		c.Lock()
		defer c.Unlock()
		if _, ok := c.reduceTasks[task.ID]; ok {
			delete(c.reduceTasks, task.ID)
			c.id++
			newTask := ReduceTask{
				Task: Task{
					ID:        c.id,
					StartedAt: time.Now(),
				},
				PartitionID: task.PartitionID,
				FileCount:   task.FileCount,
			}
			c.reduceQueue = append(c.reduceQueue, newTask)
		}
	}()
	return &task
}

func (c *Coordinator) completedReducePhaseCheck() bool {
	c.Lock()
	defer c.Unlock()
	if len(c.reduceQueue) > 0 || len(c.reduceTasks) > 0 {
		return false
	}
	return true
}

func (c *Coordinator) completedMapPhaseCheck() bool {
	c.Lock()
	defer c.Unlock()
	if c.mapped {
		return true
	}
	if len(c.mapQueue) > 0 || len(c.mapTasks) > 0 {
		return false
	}
	c.mapped = true
	return true
}

func (c *Coordinator) completeTask(id int, command CommandType) {
	c.Lock()
	defer c.Unlock()
	switch command {
	case MapCommand:
		delete(c.mapTasks, id)
	case ReduceCommand:
		delete(c.reduceTasks, id)
	}
}

// RPC handler for a worker to get the next task in the queue
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GetNextTask(args *GetNextTaskArgs, reply *GetNextTaskReply) error {
	if args.ID != -1 {
		c.completeTask(args.ID, args.Command)
	}
	reply.Partitions = c.partitions
	mapTask := c.getMapTask()
	if mapTask != nil {
		reply.MapTask = mapTask
		reply.Type = MapCommand
		reply.ID = mapTask.ID
		return nil
	}
	completed := c.completedMapPhaseCheck()
	if !completed {
		c.Lock()
		defer c.Unlock()
		c.id++
		reply.ID = c.id
		reply.Type = WaitCommand
		return nil
	}
	reduceTask := c.getReduceTask()
	if reduceTask != nil {
		reply.ReduceTask = reduceTask
		reply.Type = ReduceCommand
		reply.ID = reduceTask.ID
		return nil
	}
	if !c.completedReducePhaseCheck() {
		c.Lock()
		defer c.Unlock()
		c.id++
		reply.ID = c.id
		reply.Type = WaitCommand
		return nil
	}
	c.Lock()
	defer c.Unlock()
	c.id++
	reply.ID = c.id
	reply.Type = CompletedCommand
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
// TODO:
// after we have successfully refactored how queuing is handled
// we need to check for queue length and active tasks lengths
func (c *Coordinator) Done() bool {
	c.Lock()
	defer c.Unlock()
	return len(c.mapQueue) <= 0 && len(c.reduceQueue) <= 0 && len(c.reduceTasks) <= 0
}

func buildMapQueue(files []string) []MapTask {
	mapQueue := []MapTask{}
	for i, file := range files {
		task := MapTask{
			Task: Task{
				ID:        i,
				StartedAt: time.Now(),
			},
			File:   file,
			FileID: i,
		}
		mapQueue = append(mapQueue, task)
	}
	return mapQueue
}

func buildReduceQueue(filesCount int, partitions int) ([]ReduceTask, int) {
	reduceQueue := []ReduceTask{}
	id := filesCount
	for i := 0; i < partitions; i++ {
		id++
		reduceQueue = append(reduceQueue, ReduceTask{
			Task: Task{
				ID:        id,
				StartedAt: time.Now(),
			},
			FileCount:   filesCount,
			PartitionID: i,
		})
	}
	return reduceQueue, id
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// partitions is the number of reduce tasks to use.
func MakeCoordinator(files []string, partitions int) *Coordinator {
	var c = Coordinator{
		mapTasks:    make(map[int]MapTask),
		reduceTasks: make(map[int]ReduceTask),
	}
	c.mapQueue = buildMapQueue(files)
	reduceQueue, id := buildReduceQueue(len(files)-1, partitions)
	c.reduceQueue = reduceQueue
	c.partitions = partitions
	c.id = id
	c.server()
	return &c
}
