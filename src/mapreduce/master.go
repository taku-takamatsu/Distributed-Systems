package mapreduce

import (
	"container/list"
	"fmt"
	"sync"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func ConsumeRegisteredChannels(mr *MapReduce) {
	//consume registered workers in a separate goroutine
	//reference: https://edstem.org/us/courses/19078/discussion/1032883
	for address := range mr.registerChannel {
		//update Worker map
		mr.Workers[address] = &WorkerInfo{address}
		//hand out a job
		mr.availableWorkers <- address
	}
}

func SubmitJob(mr *MapReduce, op JobType) {
	var nJobs int
	var nOtherPhase int
	switch op {
	case "Map":
		nJobs = mr.nMap
		nOtherPhase = mr.nReduce
	case "Reduce":
		nJobs = mr.nReduce
		nOtherPhase = mr.nMap
	}

	q := make(chan int, nJobs) //use buffered channel as queue
	//reference: https://edstem.org/us/courses/19078/discussion/1070461
	for i := 0; i < nJobs; i++ {
		q <- i
	}
	var mutex sync.Mutex
	completed := 0 //maintain count of successful jobs

	for { // use inifinite loop that exits when all jobs are completed
		select {
		case jobId := <-q: //get a job from the queue
			go func() {
				address := <-mr.availableWorkers //consume an available worker
				args := DoJobArgs{mr.file, op, jobId, nOtherPhase}
				var reply DoJobReply
				ok := call(address, "Worker.DoJob", &args, &reply)
				if ok && reply.OK {
					mutex.Lock()
					completed++
					mutex.Unlock()
					mr.availableWorkers <- address //hand out another job
				} else {
					q <- jobId // retry, push back to queue
				}
			}()
		default:
			if completed >= nJobs {
				return
			}
		}
	}

	fmt.Printf("Done with job: %s", op)
}

func (mr *MapReduce) RunMaster() *list.List {
	go ConsumeRegisteredChannels(mr)

	//Master doesn't need to differentiate between the two operations
	SubmitJob(mr, "Map")
	SubmitJob(mr, "Reduce")

	return mr.KillWorkers()
}
