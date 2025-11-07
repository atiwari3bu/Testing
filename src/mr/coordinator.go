package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"


type Coordinator struct {
	// Your definitions here.
    files []string
    Completed_Jobs map[string]bool
    total_workers int
    workers map[string]int
    n_Reduce int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) GetFileName(args *ExampleArgs, reply *ExampleReply) error {
    
    if _, exists := c.workers[args.Worker_PID]; !exists {
        // Insert the key-value pair if it doesn't exist
        c.workers[args.Worker_PID] = c.total_workers 
        c.total_workers++ //increment the total worker count

        fmt.Printf("Worker : %v added\n",args.Worker_PID)
    }

    if len(c.files) > 0 {
        reply.File = c.files[0]
        reply.NReduce = c.n_Reduce
        reply.WorkerNum = c.workers[args.Worker_PID]
        fmt.Printf("File send by coord is %v\n", c.files[0])

        c.files = c.files[1:]

    }else{
        reply.Y = 400
    }

	return nil
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 500
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
    fmt.Println("Sockname is ", sockname)

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
	ret := false 

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
    fmt.Printf("Make Coordinator value of nReduce is %v\n", nReduce) 

	c := Coordinator{
        workers: make(map[string]int),
        total_workers : 0,
        n_Reduce : nReduce,
    }

	// Your code here.
    fmt.Println(files)
    
    c.files = files
	c.server()
	return &c
}
