package shardkv

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"shardmaster"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const Debug = 1

const TIMEOUT_DURATION = 1 // timeout of any infinite loops after 1 second

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format+"\n", a...)
	}
	return
}

type Op struct {
	TxId     int64  // Id set for each Operation
	ClientId int64  // Id set for Clerk
	Seq      int    // seq number
	Name     string // GET, PUT, PUTHASH, RECONFIG
	Key      string
	Val      string
	Config   shardmaster.Config // Reconfig; kv server can query based on this value
}

// Note: After a server has moved to a new view, it can leave the shards that it is not
// owning in the new view undeleted. This will simplify the server implementation.
type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID
	// local db/state indexed by shard number, followed by actual data
	// this should make it easier to get all data the shard is responsible for
	data     map[int]map[string]string // actual kv
	previous map[int]map[int64]string  // previous vals for PUTHASH; indexed by ClientId
	seenOps  map[int]map[int64]bool    // local state; indexed by TxId
	received map[int]*ReConfigArgs     // cache of configurations; indexed by config number
	logSeq   int                       // last seen Paxos seq number
	config   shardmaster.Config        // current config
	myShards map[int]bool              // all of my shards

	muReconfig sync.Mutex // mutex for reconfiguration RPC
}

//
// Paxos helper: wait for seq to be decided
//
func (kv *ShardKV) Wait(seq int) Op {
	to := 10 * time.Millisecond
	for { // keep trying
		if kv.dead {
			return Op{}
		}
		if ok, val := kv.px.Status(seq); ok {
			return val.(Op)
		}
		time.Sleep(to)
		// DPrintf("sleeping... seq=%v", seq)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

// start Paxos agreement with the Op
func (kv *ShardKV) PaxosAgreement(op Op) (string, Err) {
	DPrintf("StartPaxosAgreement: seq=%v, TxId=%v, gid=%v, me=%v", kv.logSeq+1, op.TxId, kv.gid, kv.me)
	for {
		if kv.dead {
			return "", "Error: server dead"
		}
		seq := kv.logSeq + 1
		var newOp Op
		// DPrintf("StartPaxos: calling status on seq=%v, me=%v", seq, kv.me)
		if decided, val := kv.px.Status(seq); decided {
			// decided
			newOp = val.(Op)
		} else {
			// not decided
			// DPrintf("StartPaxos: calling START on seq=%v, me=%v", seq, kv.me)
			op.Seq = seq
			kv.px.Start(seq, op) // call Start(); which will either discover the previously agreed-to-value or cause agreement to happen
			newOp = kv.Wait(seq)
		}

		DPrintf("StartPaxos: Decided TxId=%v, name=%v, seq=%v, gid=%v, me=%v", newOp.TxId, newOp.Name, newOp.Seq, kv.gid, kv.me)
		currValue, ok := kv.UpdateLog(newOp)
		// if ok != OK {
		//  // return "", ok
		// }
		kv.px.Done(seq) // this server is done with all instances <= seq
		kv.logSeq++     // try next seq number
		if newOp.Name == RECONFIG && newOp.Config.Num == op.Config.Num {
			// we're done if config numbers are the same
			DPrintf("PaxosAgreement: Config applied TxId=%v", op.TxId)
			return currValue, ok
		} else if op.TxId == newOp.TxId { // decided value has same id as op
			// we're up to date, so return
			DPrintf("Server: Decided TxId=%v, seq=%v, val=%v", op.TxId, seq, currValue)
			return currValue, ok
		}
	}
}

//
// Update local log based on decided operation
// For RECONFIG, return empty string ""
// For PUTHASH, return current value
//
func (kv *ShardKV) UpdateLog(op Op) (string, Err) {
	DPrintf("UpdateLog: seq=%v, txid=%v ,configNum=%v, currConfigNum=%v, gid=%v, me=%v", op.Seq, op.TxId, op.Config.Num, kv.config.Num, kv.gid, kv.me)
	if op.Name == RECONFIG {
		kv.ApplyReconfig(op.Config, op.TxId)
		return "", OK
	} else {
		shard := key2shard(op.Key)

		// if not our shard, return error
		if _, e := kv.myShards[shard]; !e {
			DPrintf("UpdateLog: ErrWrongGroup txId=%v, gid=%v, me=%v myShards=%v", op.TxId, kv.gid, kv.me, kv.myShards)
			return "", ErrWrongGroup
		}

		// make sure kv.data[shard] exists
		if _, e := kv.data[shard]; !e {
			kv.data[shard] = make(map[string]string)
		}
		// make sure seenOps[shard] exists
		if _, e := kv.seenOps[shard]; !e {
			kv.seenOps[shard] = make(map[int64]bool)
		}

		// could be "" if first k/v
		currValue := kv.data[shard][op.Key]

		// check duplicate
		if _, exist := kv.seenOps[shard]; exist && kv.seenOps[shard][op.TxId] {
			if op.Name == PUTHASH { // PUTHASH has to return previous value
				return kv.previous[shard][op.ClientId], OK
			}
			return currValue, OK // for GET and PUT; value doesn't really matter
		}

		if op.Name == PUT {
			kv.seenOps[shard][op.TxId] = true
			kv.data[shard][op.Key] = op.Val
		} else if op.Name == PUTHASH {
			// make sure previous[shard] exists
			if _, e := kv.previous[shard]; !e {
				kv.previous[shard] = make(map[int64]string)
			}
			kv.previous[shard][op.ClientId] = currValue
			kv.seenOps[shard][op.TxId] = true
			kv.data[shard][op.Key] = strconv.Itoa(int(hash(currValue + op.Val))) // hash
		}

		return currValue, OK
	}

}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Similar to assignment 3; no need to cache Get and just let client retry on error
	DPrintf("GET: received k=%v, TxId=%v, gid=%v, me=%v\n", args.Key, args.TxId, kv.gid, kv.me)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		TxId:     args.TxId,
		ClientId: args.ClientId,
		Name:     GET,
		Key:      args.Key}
	// DPrintf("Server: GET Starting Paxos Agreement me=%v from seq=%v", kv.me, kv.logSeq+1)
	value, err := kv.PaxosAgreement(op) // returns the seq number that it agreed to
	DPrintf("GET: Paxos complete, k=%v, v=%v txId=%v", args.Key, value, args.TxId)
	if err != OK {
		reply.Err = err
		return nil // maybe Paxos error? - let client retry
	}

	reply.Value = value
	reply.Err = err

	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	DPrintf("PUT: received k=%v, v=%v, dohash=%v, TxId=%v, gid=%v, me=%v\n", args.Key, args.Value, args.DoHash, args.TxId, kv.gid, kv.me)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// at most once semantic
	shard := key2shard(args.Key)
	if _, exist := kv.seenOps[shard]; exist && kv.seenOps[shard][args.TxId] { // seen shard?
		if args.DoHash {
			reply.PreviousValue = kv.previous[shard][args.ClientId]
		}
		reply.Err = OK
		DPrintf("SERVER: Duplicate PUT request id=%v, gid=%v, me=%v, prevErr=%v", args.TxId, kv.gid, kv.me, reply.Err)
		return nil
	}

	// prepare Paxos Op
	var op Op
	if args.DoHash {
		op = Op{
			TxId:     args.TxId,
			ClientId: args.ClientId,
			Name:     PUTHASH,
			Key:      args.Key,
			Val:      args.Value}
	} else {
		op = Op{
			TxId:     args.TxId,
			ClientId: args.ClientId,
			Name:     PUT,
			Key:      args.Key,
			Val:      args.Value}
	}
	value, err := kv.PaxosAgreement(op) // start Paxos
	if err != OK {
		reply.Err = err
		return nil
	}

	reply.PreviousValue = value
	reply.Err = err
	DPrintf("PUT: success k=%v, v=%v, TxId=%v gid=%v, me=%v\n", args.Key, args.Value, args.TxId, kv.gid, kv.me)
	return nil
}

func (kv *ShardKV) GetRemaining(shards map[int]bool) map[int]bool {
	remaining := make(map[int]bool) // find all shards that we need to wait for
	for k := range shards {
		if _, e := kv.myShards[k]; !e {
			remaining[k] = true
		}
	}
	return remaining
}

//
// Reconfiguration also requires interaction among the replica groups.
// For example, in configuration 10 group G1 may be responsible for shard S1.
// In configuration 11, group G2 may be responsible for shard S1. During the
// reconfiguration from 10 to 11, G1 must send the content of shard S1
//(the key/value pairs) to G2.
//
func (kv *ShardKV) ApplyReconfig(config shardmaster.Config, txId int64) {
	// mutex locked from parent
	DPrintf("ApplyReconfig gid=%v, me=%v, data=%v TxId=%v\n", kv.gid, kv.me, kv.data, txId)
	// skip if our local configuration number is higher than what was requested
	// needed for TestConcurrentUnreliable
	if config.Num <= kv.config.Num {
		return
	}

	// get shards we should send to other groups; compare with last logged configuration
	// Ref: https://edstem.org/us/courses/19078/discussion/1366911
	myShards := make(map[int]bool)              // to update local state
	otherShards := make(map[int64]ReConfigArgs) // store args to send to other groups
	for shard, gid := range config.Shards {     // loop over the new shard configurations
		if kv.gid == gid {
			// keep track of all the shards that should be part of our replica group
			myShards[shard] = true
		} else if yes := kv.myShards[shard]; yes {
			// if shard was one of my shards, then we have to transfer this over to the new gid
			if _, exist := otherShards[gid]; !exist {
				// generate RPC args to transfer kv to other groups
				args := ReConfigArgs{
					Data:     make(map[int]map[string]string),
					SeenOps:  make(map[int]map[int64]bool),
					Previous: make(map[int]map[int64]string)}
				args.SeenOps[shard] = kv.seenOps[shard]
				args.Previous[shard] = kv.previous[shard]
				args.Data[shard] = kv.data[shard]
				otherShards[gid] = args // store arg for new GID
				// DPrintf("Reconfigure: othershards[%v]=%v", gid, otherShards[gid])
			} else {
				// if exist, append states for this shard
				otherShards[gid].SeenOps[shard] = kv.seenOps[shard]
				otherShards[gid].Previous[shard] = kv.previous[shard]
				otherShards[gid].Data[shard] = kv.data[shard]
			}
		}
	}
	// Now we have:
	// myShards: boolean holding all shard numbers for this server
	// otherShards: holds all GID to kv pairs that other groups need; we'll send these to all server in each GIDs
	DPrintf("ApplyReconfig: prepared gid=%v, me=%v, myshards=%v", kv.gid, kv.me, myShards)
	DPrintf("ApplyReconfig: prepared gid=%v, me=%v, otherShards=%v", kv.gid, kv.me, otherShards)
	if kv.config.Num > 0 { // don't send/wait if first configuration, or else we'll be waiting indefinitely
		// send shards to other replica groups
		kv.SendShards(&otherShards, &txId, &config)
		// we shouldn't proceed if we haven't gotten all the data for our shards
		// But how do we know when we've received all of the data?
		// Ref: https://edstem.org/us/courses/19078/discussion/1383608
		remaining := kv.GetRemaining(myShards)
		// we'll have to wait till we receive RPC calls from these
		DPrintf("ApplyReconfig: prevShards=%v, txId=%v", kv.myShards, txId)
		kv.WaitForShards(&remaining, &config)
	}
	kv.myShards = myShards
	kv.config = config
	DPrintf("ApplyReconfig: received all shards for txId=%v", txId)
}

//
// Send my shards over RPC
// Timeout each RPC call if TIMEOUT_DURATION has passed
//
func (kv *ShardKV) SendShards(shards *map[int64]ReConfigArgs, txId *int64, config *shardmaster.Config) {
	for gid, args := range *shards {
		// for each server in this gid
		args.TxId = *txId // set TxId here, one for each GID
		args.Config = *config
		for _, srv := range config.Groups[gid] {
			DPrintf("SendShards: Othershards; sending data=%v to=%v, txid=%v, gid=%v, me=%v, args=%v", args.Data, gid, txId, kv.gid, kv.me, args)
			start := time.Now()
			for !kv.dead { // keep trying until we succeed
				reply := &ReConfigReply{}
				ok := call(srv, "ShardKV.ReceiveReconfig", args, &reply)
				// what to do if destination server is dead?
				// for now break
				if ok && reply.Err == OK {
					break
				} else if time.Until(start).Seconds() < -TIMEOUT_DURATION {
					// if > TIMEOUT_DURATION seconds; assume server is dead, so break
					// Ref: https://www.geeksforgeeks.org/time-until-function-in-golang-with-examples/
					DPrintf("SendShards: timeout gid=%v, me=%v", kv.gid, kv.me)
					break
				}
				time.Sleep(100 * time.Millisecond)
				DPrintf("SendShards: retrying txId=%v to gid=%v reply=%v", txId, gid, reply)
			}
		}
	}
}

//
// Wait for shards from other replica groups;
// Populate config cache
//
func (kv *ShardKV) WaitForShards(remaining *map[int]bool, config *shardmaster.Config) {
	for len(*remaining) > 0 { // wait until all remaining shards are received
		if kv.dead {
			break
		}
		// see if we have received shards for this config number
		// use separate locks
		// Ref: https://edstem.org/us/courses/19078/discussion/1379993
		kv.muReconfig.Lock()
		if args, e := kv.received[config.Num]; e {
			// if we have, we're now ready to update our local database
			for shard := range *remaining { // for each of the remaining shards, check if we just received that data
				// start := time.Now()
				// for the remaining shards, update
				if _, yes := args.Data[shard]; yes {
					// make sure kv.data[shard] exists
					if _, exist := kv.data[shard]; !exist {
						kv.data[shard] = make(map[string]string)
					}
					for k, v := range args.Data[shard] { // copy data
						kv.data[shard][k] = v
					}
					// make sure kv.seenOps[shard] exists
					if _, exist := kv.seenOps[shard]; !exist {
						kv.seenOps[shard] = make(map[int64]bool)
					}
					for k, v := range args.SeenOps[shard] { // copy state
						kv.seenOps[shard][k] = v
					}
					// make sure kv.previous[shard] exists
					if _, exist := kv.previous[shard]; !exist {
						kv.previous[shard] = make(map[int64]string)
					}
					for k, v := range args.Previous[shard] {
						kv.previous[shard][k] = v
					}
					delete(*remaining, shard) // we don't have to wait for this shard anymore
				}
				// if time.Until(start).Seconds() < -TIMEOUT_DURATION {
				// 	DPrintf("WaitForShards: timeout!")
				// 	break
				// }
			}
		}
		kv.muReconfig.Unlock()
		if len(*remaining) == 0 { // done if we received all shards
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) ReceiveReconfig(args ReConfigArgs, reply *ReConfigReply) error {
	// store the data that we received into local cache;
	// only update local database when we're ready to process it
	// Ref: https://edstem.org/us/courses/19078/discussion/1383608?comment=3143481

	// args received from other replica groups by RPCs
	// args.Data, args.SeenOps and args.Previous should only have data wrt. the shards that this GID owns
	kv.muReconfig.Lock()
	defer kv.muReconfig.Unlock()
	DPrintf("ReceiveReconfig: gid=%v, me=%v, txId=%v, data=%v, args.config=%v", kv.gid, kv.me, args.TxId, args.Data, args.Config.Num)
	// update received cache
	receivedArgs, e := kv.received[args.Config.Num]
	if !e {
		receivedArgs = &ReConfigArgs{
			Data:     make(map[int]map[string]string),
			SeenOps:  make(map[int]map[int64]bool),
			Previous: make(map[int]map[int64]string)}
	} else {
		DPrintf("ReceiveReconfig: received=%v", *kv.received[args.Config.Num])
	}

	for shard, data := range args.Data { // data is indexed by shard
		if _, yes := args.Data[shard]; yes {
			// ensure we've initialized each shard
			if _, exist := receivedArgs.Data[shard]; !exist {
				receivedArgs.Data[shard] = make(map[string]string)
			}
			for k, v := range data { // copy data
				receivedArgs.Data[shard][k] = v
			}
			if _, exist := receivedArgs.SeenOps[shard]; !exist {
				receivedArgs.SeenOps[shard] = make(map[int64]bool)
			}
			for k, v := range args.SeenOps[shard] { // copy state
				receivedArgs.SeenOps[shard][k] = v
			}
			if _, exist := receivedArgs.Previous[shard]; !exist {
				receivedArgs.Previous[shard] = make(map[int64]string)
			}
			for k, v := range args.Previous[shard] { // copy previous vals
				receivedArgs.Previous[shard][k] = v
			}
		}
	}
	receivedArgs.TxId = args.TxId
	receivedArgs.Config = args.Config
	// store config in cache
	// so that any ops waiting for this shard can consume it
	kv.received[args.Config.Num] = receivedArgs
	DPrintf("ReceiveReconfig: Applied gid=%v, me=%v, txId=%v, data=%v", kv.gid, kv.me, args.TxId, kv.received[args.Config.Num].Data)
	reply.Err = OK
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	// check if updated
	kv.mu.Lock()
	config := kv.sm.Query(-1)
	if config.Num > kv.config.Num {
		kv.mu.Unlock()
		DPrintf("Tick: Reconfigure gid=%v, me=%v, config=%v; applying sequentially", kv.gid, kv.me, config)
		// apply configuration changes sequentially
		// Ref: https://edstem.org/us/courses/19078/discussion/1379949
		// if not we were missing a bunch of configs
		for i := kv.config.Num + 1; i <= config.Num; i++ {
			kv.Reconfigure(kv.sm.Query(i))
		}
	} else {
		kv.mu.Unlock()
	}
}

//
// Apply reconfiguration into Paxos log
//
func (kv *ShardKV) Reconfigure(config shardmaster.Config) {
	// log reconfig op into our log; only apply when caught up
	kv.mu.Lock()
	defer kv.mu.Unlock()
	txId := nrand()
	DPrintf("Reconfigure: Received gid=%v, me=%v, created TxId=%v\n", kv.gid, kv.me, txId)
	// we'll reconfigure through Paxos rounds
	op := Op{
		TxId:   txId,
		Name:   RECONFIG,
		Config: config}
	kv.PaxosAgreement(op)
	DPrintf("Reconfigure: Done gid=%v, me=%v, txId=%v, myShards=%v", kv.gid, kv.me, txId, kv.myShards)
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})
	gob.Register(ReConfigArgs{}) // marshall Reconfig RPC calls

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.data = make(map[int]map[string]string)
	kv.seenOps = make(map[int]map[int64]bool)
	kv.previous = make(map[int]map[int64]string)
	kv.received = make(map[int]*ReConfigArgs)
	kv.logSeq = -1
	kv.config = shardmaster.Config{Num: 0} // local config
	kv.myShards = make(map[int]bool)
	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
