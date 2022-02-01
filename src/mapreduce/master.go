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

// custom struct to easily send back responses
type WorkerResponse struct {
	status bool //maintain status of each worker job
	jobId  int  //the job ID
}

//helper function to send RPC
func SendRPC(mr *MapReduce, address string, op JobType, jobId int, wg *sync.WaitGroup) error {
	var otherPhase int
	switch op {
	case "Map":
		otherPhase = mr.nReduce
	case "Reduce":
		otherPhase = mr.nMap
	}

	args := &DoJobArgs{mr.file, op, jobId, otherPhase}
	var reply DoJobReply

	ok := call(address, "Worker.DoJob", args, &reply)

	wg.Done() //release
	//send response back to detect errors
	mr.workerResponses <- &WorkerResponse{reply.OK && ok, jobId}

	if ok && reply.OK {
		mr.availableWorkers <- address //hand out another job
	}
	return nil
}

func ConsumeRegisteredChannels(mr *MapReduce) {
	for address := range mr.registerChannel {
		//update Worker map
		mr.Workers[address] = &WorkerInfo{address}
		//hand out a job
		mr.availableWorkers <- address
	}
}

func SubmitJob(mr *MapReduce, op JobType) {
	var nJobs int
	switch op {
	case "Map":
		nJobs = mr.nMap
	case "Reduce":
		nJobs = mr.nReduce
	}

	var wg sync.WaitGroup

	q := list.New() //queue to maintain count of jobs
	for i := 0; i < nJobs; i++ {
		q.PushBack(i)
	}
	wg.Add(nJobs) //add number of jobs to wait for

	for q.Len() > 0 {
		select {
		case address := <-mr.availableWorkers: //consume an available worker
			jobId := q.Remove(q.Front()).(int)
			go SendRPC(mr, address, op, jobId, &wg) //send RPC as goroutine
		case response := <-mr.workerResponses: //consume completed workers
			if !response.status { //error, retry
				q.PushFront(response.jobId)
				wg.Add(1)
			}
		}
	}
	wg.Wait() //wait for all go routines to finish
}

func (mr *MapReduce) RunMaster() *list.List {
	//consume registered workers in a separate goroutine
	//reference: https://edstem.org/us/courses/19078/discussion/1032883
	go ConsumeRegisteredChannels(mr)

	//Master doesn't need to differentiate between the two operations
	SubmitJob(mr, "Map")
	SubmitJob(mr, "Reduce")

	return mr.KillWorkers()
}
