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

	q := list.New() //queue to maintain count of jobs
	for i := 0; i < nJobs; i++ {
		q.PushBack(i)
	}
	var mutex sync.Mutex
	completed := 0
	for {
		address := <-mr.availableWorkers //consume an available worker
		if q.Len() > 0 {
			go func() {
				jobId := q.Remove(q.Front()).(int)
				args := DoJobArgs{mr.file, op, jobId, nOtherPhase}
				var reply DoJobReply
				ok := call(address, "Worker.DoJob", &args, &reply)
				mutex.Lock()
				defer mutex.Unlock()
				if ok && reply.OK {
					completed++
					mr.availableWorkers <- address //hand out another job
				} else {
					q.PushFront(jobId) //not thread-safe; reference: https://github.com/golang/go/issues/25105
				}
			}()
		}
		if completed >= nJobs {
			break
		}
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	go ConsumeRegisteredChannels(mr)

	//Master doesn't need to differentiate between the two operations
	SubmitJob(mr, "Map")
	SubmitJob(mr, "Reduce")

	return mr.KillWorkers()
}
