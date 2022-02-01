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

//helper function to submit job
func SendRPC(mr *MapReduce, address string, op string, jobCount int, wg *sync.WaitGroup) error {
	workerInfo := mr.Workers[address]
	var otherPhase int
	var operation JobType
	switch op {
	case "Map":
		otherPhase = mr.nReduce
		operation = "Map"
	case "Reduce":
		otherPhase = mr.nMap
		operation = "Reduce"
	}
	args := &DoJobArgs{
		File:          mr.file,
		Operation:     operation,
		JobNumber:     jobCount,
		NumOtherPhase: otherPhase}

	var reply DoJobReply

	status := call(workerInfo.address, "Worker.DoJob", args, &reply)
	wg.Done() //release
	if status {
		//hand out another job with idle worker address
		mr.availableWorkers <- workerInfo.address
	}
	return nil
}

func consumeRegisteredChannels(mr *MapReduce) {
	for address := range mr.registerChannel {
		//update Worker map
		mr.Workers[address] = &WorkerInfo{address}
		//hand out a job
		mr.availableWorkers <- address
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	//consume registered workers in a separate goroutine
	//reference: https://edstem.org/us/courses/19078/discussion/1032883
	go consumeRegisteredChannels(mr)

	//maintain count of jobs
	jobCount := 0
	var mutex sync.Mutex  //use mutex to ensure no race condition
	var wg sync.WaitGroup //waitgroup for all workers

	//MAP
	wg.Add(mr.nMap) //add number of map operations to the waitgroup
	for jobCount < mr.nMap {
		address := <-mr.availableWorkers              //consume an available worker
		go SendRPC(mr, address, "Map", jobCount, &wg) //send RPC as goroutine
		// increment counter in a mutual exclusive lock
		mutex.Lock()
		if jobCount < mr.nMap { //foolproof
			jobCount++
		}
		mutex.Unlock()
	}
	wg.Wait() //wait for MAP to finish before continuing
	fmt.Println("Map Operation Completed:", jobCount, "jobs out of", mr.nMap)

	//REDUCE
	jobCount = 0       //reset job count
	wg.Add(mr.nReduce) //setup wait groups
	for jobCount < mr.nReduce {
		address := <-mr.availableWorkers
		go SendRPC(mr, address, "Reduce", jobCount, &wg) //send RPC as goroutine
		mutex.Lock()
		if jobCount < mr.nReduce {
			jobCount++
		}
		mutex.Unlock()
	}
	wg.Wait()
	fmt.Println("Reduce Operation Completed:", jobCount, "jobs out of", mr.nReduce)

	return mr.KillWorkers()
}
