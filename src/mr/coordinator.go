package mr

import (
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
)

// 任务的唯一标识符、状态（例如，等待、运行、完成或失败）和位置（哪个Worker节点上）
type Task struct {
	TaskType  TaskType
	TaskNum   int // 任务的唯一标识符
	Filename  string
	Filenames []string
}

type Coordinator struct {
	// Your definitions here.
	mapTasks    map[int]map[int]Task // 假设Task是一个任务的结构体
	reduceTasks map[int]map[int]Task // 状态（例如，等待0、运行1、完成2或失败3）
	nReduce     int
	nMap        int
	taskMux     sync.Mutex
	reduceF     bool
}

func (c *Coordinator) creatReduces() error {
	for i := 0; i < c.nReduce; i++ {
		pattern := fmt.Sprintf("mr-*-%d", i)

		// filepath.Glob返回匹配特定模式的文件名
		matches, err := filepath.Glob(pattern)
		if err != nil {
			fmt.Println("Error: CreatReduces", err)
			return nil
		}
		t := Task{TaskType: ReduceTask, TaskNum: i, Filenames: matches}
		// matches是一个包含所有匹配文件名的字符串切片
		c.reduceTasks[0][i] = t
	}

	return nil
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) WcReply(args *WorkArgs, reply *WorkReply) error {
	c.taskMux.Lock()
	defer c.taskMux.Unlock()
	if len(c.mapTasks[0]) == 0 && len(c.reduceTasks[0]) == 0 {
		// 当前没有空闲任务
		reply.waitF = true
	} else {
		reply.waitF = false
		if len(c.mapTasks[2]) == c.nMap {
			// map任务结束
			//fmt.Println("reduce1")
			// 分配任务
			for key, value := range c.reduceTasks[0] {
				reply.Task = value
				delete(c.reduceTasks[0], key)
				break // 立即退出循环
			}
			c.reduceTasks[1][reply.Task.TaskNum] = reply.Task
			reply.NReduce = c.nReduce
			go c.checkWork(reply.Task)
		} else {
			//fmt.Println("map1")
			for key, value := range c.mapTasks[0] {
				reply.Task = value
				delete(c.mapTasks[0], key)
				break // 立即退出循环
			}
			c.mapTasks[1][reply.Task.TaskNum] = reply.Task
			reply.NReduce = c.nReduce
			go c.checkWork(reply.Task)
		}
	}
	return nil
}

func (c *Coordinator) WorkDone(args *WorkArgs, reply *WorkReply) error {
	c.taskMux.Lock()
	defer c.taskMux.Unlock()
	if args.Done == true {
		task := args.Task
		if task.TaskType == MapTask {
			if _, ok := c.mapTasks[2][task.TaskNum]; ok == false {
				// 从未收到过该消息
				delete(c.mapTasks[1], task.TaskNum)
				c.mapTasks[2][task.TaskNum] = task
				if c.reduceF == false && len(c.mapTasks[2]) == c.nMap {
					// 创建reduces任务
					c.creatReduces()
					c.reduceF = true
					// todo : 分配新reduce任务了
				}
				// todo : 分配新map任务
			}
		} else {
			if _, ok := c.reduceTasks[2][task.TaskNum]; ok == false {
				// 从未收到过该消息
				delete(c.reduceTasks[1], task.TaskNum)
				c.reduceTasks[2][task.TaskNum] = task
				if len(c.reduceTasks[2]) == c.nReduce {
					// todo : 所有任务结束，你可以断开了
					reply.Done = true
					return nil
				} else {
					reply.Done = false
					// todo : 分配新的reduce任务
				}
			}
		}
	}

	// 分配任务
	if len(c.mapTasks[0]) != 0 {
		// 分配map任务
		for key, value := range c.mapTasks[0] {
			reply.Task = value
			c.mapTasks[1][value.TaskNum] = value
			go c.checkWork(value)
			delete(c.mapTasks[0], key)
			break // 立即退出循环
		}
		reply.NReduce = c.nReduce
	} else if len(c.reduceTasks[0]) != 0 {
		// 分配reduce任务
		for key, value := range c.reduceTasks[0] {
			reply.Task = value
			c.reduceTasks[1][value.TaskNum] = value
			go c.checkWork(value)
			delete(c.reduceTasks[0], key)
			break // 立即退出循环
		}
		reply.NReduce = c.nReduce
	} else {
		reply.waitF = true
	}
	return nil
}

func (c *Coordinator) checkWork(task Task) bool {
	c.taskMux.Lock()
	var ok bool
	t := task.TaskType
	if t == MapTask {
		_, ok = c.mapTasks[2][task.TaskNum]
	} else {
		_, ok = c.reduceTasks[2][task.TaskNum]
	}
	c.taskMux.Unlock()
	i := 0
	for ok == false {
		if i >= 5 {
			//fmt.Printf("task%d is crashed\n", task.TaskNum)
			// todo : 任务重新分配
			c.taskMux.Lock()
			if t == MapTask {
				delete(c.mapTasks[1], task.TaskNum)
				c.mapTasks[0][task.TaskNum] = task
			} else {
				delete(c.reduceTasks[1], task.TaskNum)
				c.reduceTasks[0][task.TaskNum] = task
			}
			c.taskMux.Unlock()
			return false
		}
		time.Sleep(2 * time.Second)
		i++
		c.taskMux.Lock()
		if t == MapTask {
			_, ok = c.mapTasks[2][task.TaskNum]
		} else {
			_, ok = c.reduceTasks[2][task.TaskNum]
		}
		c.taskMux.Unlock()
	}
	return true
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	go http.Serve(l, nil)
}

func (c *Coordinator) init() {
	someSize := 4
	// 为外层map的每个键初始化内层map
	for i := 0; i < someSize; i++ {
		c.mapTasks[i] = make(map[int]Task)
		c.reduceTasks[i] = make(map[int]Task)
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.taskMux.Lock()
	defer c.taskMux.Unlock()
	if len(c.reduceTasks[2]) == c.nReduce {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{mapTasks: make(map[int]map[int]Task), reduceTasks: make(map[int]map[int]Task), nReduce: nReduce}
	// Your code here.
	c.init()
	for i, file := range files {
		mapTask := Task{TaskType: MapTask, TaskNum: i, Filename: file}
		c.mapTasks[0][i] = mapTask
	}
	c.nMap = len(c.mapTasks[0])
	c.server()
	return &c
}
