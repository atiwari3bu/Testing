package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "strconv"

var n_Reduce int
var worker_num int

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

/*
// for sorting by key.
type bykey KeyValue

// for sorting by key.
func (a bykey) len() int           { return len(a) }
func (a bykey) swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a bykey) less(i, j int) bool { return a[i].key < a[j].key }
*/

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func saveIntermediateToFile(intermediate []KeyValue, filename string) {
    for reduceTaskNum := 0; reduceTaskNum < n_Reduce; reduceTaskNum++ {
        reduceTaskIntermediate := []KeyValue{}

        for _, kv := range intermediate {
            if ihash(kv.Key) % n_Reduce == reduceTaskNum {
                reduceTaskIntermediate = append(reduceTaskIntermediate, kv)
            }
        }

        if len(reduceTaskIntermediate) > 0 {
            // Create filename in the format mr-X-Y
            intermediateFileName := fmt.Sprintf("mr-%d-%d", worker_num, reduceTaskNum)

            fmt.Printf("Intermediate file saved as %v\n", intermediateFileName)
            file, err := os.Create(intermediateFileName)
            if err != nil {
                fmt.Printf("Error creating file %s: %v\n", intermediateFileName, err)
                continue
            }
            defer file.Close()

            // Encode the intermediate key-value pairs for this reduce task
            encoder := json.NewEncoder(file)
            err = encoder.Encode(reduceTaskIntermediate)
            if err != nil {
                fmt.Printf("Error encoding JSON for %s: %v\n", intermediateFileName, err)
            }
        }
    }
}

/*
func saveIntermediateToFile(intermediate []KeyValue, filename string){

    // Generate a unique file name using the process ID (PID)
    fileName := fmt.Sprintf("intermediate_%s.json", filename)
    fmt.Println("Generated unique file name:", fileName)

    // Create a new file with the unique name
    file, err := os.Create(fileName)
    if err != nil {
        fmt.Println("Error creating file:", err)
        return
    }
    defer file.Close()

    // Encode the map into JSON and write it to the file
    encoder := json.NewEncoder(file)
    err = encoder.Encode(intermediate)
    if err != nil {
        fmt.Println("Error encoding JSON:", err)
        return
    }

}
*/

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Efile_to_process xample RPC to the coordinator.
	//CallExample()
    filename := getFile()
    fmt.Printf("File to process is  %s\n", filename)

    intermediate := []KeyValue{}
    file, err := os.Open(filename)
    if err != nil {
        log.Fatalf("cannot open %v", filename)
    }
    content, err := ioutil.ReadAll(file)
    if err != nil {
        log.Fatalf("cannot read %v", filename)
    }
    file.Close()
    kva := mapf(filename, string(content))
    intermediate = append(intermediate, kva...)

    saveIntermediateToFile(intermediate,filename)
    Worker(mapf, reducef) 
}


func getFile() string{

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
    // ask for the filename
	args.X = 100
    args.Worker_PID = strconv.Itoa(os.Getuid())

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetFileName", &args, &reply)
	if ok {
		// reply.Y should be 100.
        if reply.Y == 400 { //no file to process
            os.Exit(0)
        }
        n_Reduce = reply.NReduce
        worker_num = reply.WorkerNum

        return reply.File

	} else {
		fmt.Printf("call failed!\n")
        return "nofile"
	}
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
