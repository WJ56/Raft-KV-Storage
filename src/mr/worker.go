package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// Your worker implementation here.
	// declare an argument structure.
	args := WorkArgs{}

	// declare a reply structure.
	reply := WorkReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.WcReply", &args, &reply)
	for ok == true && reply.Done == false {
		if reply.waitF == false {
			// 有task了，不用等待
			args.Done = true
			t := reply.Task
			if t.TaskType == MapTask {
				//fmt.Println("MapTask")
				filename := t.Filename
				file, err := os.Open(filename)
				if err != nil {
					args.Done = false
				} else {
					content, err := ioutil.ReadAll(file)
					file.Close()
					if err != nil {
						args.Done = false
					} else {
						kva := mapf(filename, string(content))

						sort.Sort(ByKey(kva))

						nReduce := reply.NReduce
						// 创建一个切片的切片来代表buckets
						buckets := make([][]KeyValue, nReduce)

						// 填充buckets，具体的逻辑取决于你的应用
						for _, kv := range kva {
							bucketIndex := ihash(kv.Key) % nReduce
							buckets[bucketIndex] = append(buckets[bucketIndex], kv)
						}
						for ReduceTaskNum, bucketContent := range buckets {
							// 使用Sprintf生成格式化的字符串并返回
							intermediateFileName := fmt.Sprintf("mr-%d-%d", t.TaskNum, ReduceTaskNum)
							intermediateFile, _ := os.Create(intermediateFileName)
							enc := json.NewEncoder(intermediateFile)
							for _, kv := range bucketContent {
								err := enc.Encode(&kv)
								if err != nil {
									// 处理错误
									fmt.Printf("write intermediateFile failed!\n")
									args.Done = false
								}
							}
						}
					}
				}
			} else {
				//fmt.Println("ReduceTask")
				var kva []KeyValue
				for _, filename := range t.Filenames {
					file, err := os.Open(filename)
					if err != nil {
						args.Done = false
					} else {
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
				}
				sort.Sort(ByKey(kva))

				// 创建临时文件
				tempFile, err := ioutil.TempFile("", "temp")
				if err != nil {
					args.Done = false
				} else {
					tempFilePath := tempFile.Name()
					//
					// call Reduce on each distinct key in intermediate[],
					// and print the result to mr-out-0.
					//
					// 向临时文件写入数据
					i := 0
					for i < len(kva) {
						j := i + 1
						for j < len(kva) && kva[j].Key == kva[i].Key {
							j++
						}
						values := []string{}
						for k := i; k < j; k++ {
							values = append(values, kva[k].Value)
						}
						output := reducef(kva[i].Key, values)

						// this is the correct format for each line of Reduce output.
						fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

						i = j
					}
					tempFile.Close()

					// 现在，finalPath 包含了之前写入的数据
					finalPath := fmt.Sprintf("./mr-out-%d", t.TaskNum)
					err = os.Rename(tempFilePath, finalPath)
					if err != nil {
						args.Done = false
					} else {
						os.Remove(tempFilePath)
					}
				}
			}
			reply = WorkReply{}
			args.Task = t
			ok = call("Coordinator.WorkDone", &args, &reply)
		} else {
			// 需要等待任务
			fmt.Print("work wait 5s")
			time.Sleep(5 * time.Second)
			reply = WorkReply{}
			ok = call("Coordinator.WcReply", &args, &reply)
		}
	}
	//fmt.Printf("call failed!\n")
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
