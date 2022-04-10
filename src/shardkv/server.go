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
	// Reconfig args
	Previous map[int]map[int64]string
	SeenOps  map[int]map[int64]bool
	Data     map[int]map[string]string
	Gid      int64
	Config   shardmaster.Config // kv server can query based on this value
}

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
	myServer string                    // servers[me]
	logSeq   int
	config   shardmaster.Config // current config
	myShards map[int]bool       // all of my shards
	servers  []string
}

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
		seq := kv.logSeq + 1
		if kv.dead {
			return "", "Error: server dead"
		}
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
		currValue, _ := kv.UpdateLog(newOp)
		// if ok != OK {
		// 	// return "", ok
		// }
		kv.px.Done(seq) // this server is done with all instances <= seq
		kv.logSeq++     // try next seq number

		if newOp.Name == RECONFIG && newOp.Config.Num == op.Config.Num {
			// we're done if config numbers are the same
			DPrintf("PaxosAgreement: Config applied TxId=%v", op.TxId)
			return currValue, OK
		} else if op.TxId == newOp.TxId { // decided value has same id as op
			// we're up to date, so return
			// for PutHash, this would equate to the previous value
			DPrintf("Server: Decided TxId=%v, seq=%v, val=%v", op.TxId, seq, currValue)
			return currValue, OK
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
		if !kv.myShards[shard] {
			return "", ErrWrongGroup
		}

		// make sure kv.data[shard] exists
		if _, e := kv.data[shard]; !e {
			kv.data[shard] = make(map[string]string)
		}
		if _, e := kv.seenOps[shard]; !e {
			kv.seenOps[shard] = make(map[int64]bool)
		}
		currValue := kv.data[shard][op.Key]
		if op.Name == PUT {
			// make sure seenOps[shard] exists
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
	kv.mu.Lock()
	DPrintf("GET: received k=%v, TxId=%v, gid=%v, me=%v\n", args.Key, args.TxId, kv.gid, kv.me)
	op := Op{
		TxId:     args.TxId,
		ClientId: args.ClientId,
		Name:     GET,
		Key:      args.Key}
	// DPrintf("Server: GET Starting Paxos Agreement me=%v from seq=%v", kv.me, kv.logSeq+1)
	value, ok := kv.PaxosAgreement(op) // returns the seq number that it agreed to
	DPrintf("GET: paxos complete, k=%v, v=%v txId=%v", args.Key, value, args.TxId)
	if ok != OK {
		kv.mu.Unlock()
		reply.Err = ok
		return nil // maybe Paxos error? - let client retry
	}
	// check if we're have the right shard
	shard := key2shard(args.Key)
	if !kv.myShards[shard] { // wrong shard
		DPrintf("GET: wrong shard gid=%v, me=%v shard=%v, myShards=%v", kv.gid, kv.me, shard, kv.myShards)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return nil
	}

	reply.Value = value
	reply.Err = OK
	kv.mu.Unlock()

	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	DPrintf("PUT: received k=%v, v=%v, dohash=%v, TxId=%v, gid=%v, me=%v\n", args.Key, args.Value, args.DoHash, args.TxId, kv.gid, kv.me)

	shard := key2shard(args.Key)
	// at most once semantic
	if _, shardExist := kv.seenOps[shard]; shardExist { // seen shard?
		if _, exist := kv.seenOps[shard][args.TxId]; exist { // duplicate transaction?
			if args.DoHash {
				reply.PreviousValue = kv.previous[shard][args.ClientId]
			}
			reply.Err = OK
			// DPrintf("SERVER: Duplicate PUT request id=%v, prevErr=%v", args.Id, reply.Err)
			kv.mu.Unlock()
			return nil
		}
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
	value, ok := kv.PaxosAgreement(op) // start paxos
	if ok != OK {
		kv.mu.Unlock()
		return nil
	}

	// check shard
	if !kv.myShards[shard] { // wrong shard
		reply.Err = ErrWrongGroup
	} else {
		reply.Err = OK
		reply.PreviousValue = value
	}

	DPrintf("PUT: success k=%v, v=%v, TxId=%v gid=%v, me=%v\n", args.Key, args.Value, args.TxId, kv.gid, kv.me)
	kv.mu.Unlock()
	return nil
}

func (kv *ShardKV) GetShards(shards [shardmaster.NShards]int64, gid int64) map[int]bool {
	myShards := make(map[int]bool)
	for s, _gid := range shards {
		if gid == _gid {
			myShards[s] = true
		}
	}
	return myShards
}

//
// Reconfiguration also requires interaction among the replica groups.
// For example, in configuration 10 group G1 may be responsible for shard S1.
// In configuration 11, group G2 may be responsible for shard S1. During the
// reconfiguration from 10 to 11, G1 must send the content of shard S1
//(the key/value pairs) to G2.
//

func (kv *ShardKV) ApplyReconfig(config shardmaster.Config, txId int64) {
	DPrintf("ApplyReconfig gid=%v, me=%v, data=%v TxId=%v\n", kv.gid, kv.me, kv.data, txId)
	// send shards to new set of replica groups
	// find new gid where srv is located
	if config.Num <= kv.config.Num { // skip if reconfiguration's moved forward
		return
	}
	// get shards we should send to other groups
	// https://edstem.org/us/courses/19078/discussion/1366911
	myShards := make(map[int]bool)              // to update local state
	otherShards := make(map[int64]ReConfigArgs) // store args to send to other groups
	// kv.mu.Lock()
	log.Printf("ApplyReconfig gid=%v, me=%v, txId=%v, Getting Shards", kv.gid, kv.me, txId)
	for shard, gid := range config.Shards { // loop over the new shard configuration
		if kv.gid == gid {
			// keep track of all the shards that should be sent to our replicas
			myShards[shard] = true
		} else if yes := kv.myShards[shard]; yes {
			// if shard was one of my shards, then we have to transfer this
			// over to the new gid
			if _, exist := otherShards[gid]; !exist {
				// generate RPC args to transfer kv to other groups
				args := ReConfigArgs{
					From:     kv.myServer,
					Data:     make(map[int]map[string]string),
					SeenOps:  make(map[int]map[int64]bool),
					Previous: make(map[int]map[int64]string),
					Config:   config}
				args.SeenOps[shard] = kv.seenOps[shard]
				args.Previous[shard] = kv.previous[shard]
				args.Data[shard] = kv.data[shard]
				otherShards[gid] = args // store arg for new GID
				// DPrintf("Reconfigure: othershards[%v]=%v", gid, otherShards[gid])
			} else {
				// if exist, append states
				otherShards[gid].SeenOps[shard] = kv.seenOps[shard]
				otherShards[gid].Previous[shard] = kv.previous[shard]
				otherShards[gid].Data[shard] = kv.data[shard]
			}
		}
	}

	// Now we have:
	// myShards: boolean holding all shard numbers for this server
	// otherShards: holds all GID to kv pairs that other groups need
	// 		we'll send these to all server in each GIDs
	DPrintf("ApplyReconfig: prepared gid=%v, me=%v, myshards=%v", kv.gid, kv.me, myShards)
	DPrintf("ApplyReconfig: prepared gid=%v, me=%v, otherShards=%v", kv.gid, kv.me, otherShards)

	for gid, args := range otherShards {
		// for each server in this gid
		args.TxId = txId // set TxId here, one for each GID
		for _, srv := range config.Groups[gid] {
			DPrintf("ApplyReconfig: Othershards; sending data=%v to=%v, txid=%v, gid=%v, me=%v", args.Data, gid, txId, kv.gid, kv.me)
			for { // keep trying until we succeed
				reply := ReConfigReply{}
				ok := call(srv, "ShardKV.ReceiveReconfig", args, &reply)
				// what to do if destination server is dead?
				// for now break
				if !ok || reply.Err == OK {
					break
				}
				time.Sleep(100 * time.Millisecond)
				DPrintf("ApplyReconfig: retrying txId=%v to gid=%v reply=%v", txId, gid, reply)
			}
		}
	}

	// seems like we need to set the configs for all replicas if first configuration
	// or else configurations won't move forward
	if config.Num == 1 {
		kv.myShards = myShards
		kv.config = config
	}
	// kv.mu.Unlock()
	DPrintf("ApplyReconfig: gid=%v, me=%v TxId=%v Done!", kv.gid, kv.me, txId)
	// kv.mu.Unlock()
}

/*
Gid = 101 joins

tick(): 102 notices reconfig, config num = 2,
	but has no data as all data is in gid=100;
	should not do anything?
tick(): 100 notices reconfig, config num = 2,
	has data, so should store in Paxos, and reconfigure
tick(): 101 notices reconfig, config num = 2,
	has no data to update as we are waiting for data from 100

*/
func (kv *ShardKV) Reconfigure(config shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	txId := nrand()
	DPrintf("Reconfigure: Received gid=%v, me=%v, created TxId=%v\n", kv.gid, kv.me, txId)
	// we'll reconfigure through Paxos rounds
	op := Op{
		TxId:   txId,
		Name:   RECONFIG,
		Config: config}
	_, ok := kv.PaxosAgreement(op)
	if ok != OK {
		return
	}

	DPrintf("Reconfigure: Done gid=%v, me=%v, txId=%v", kv.gid, kv.me, txId)
}

func (kv *ShardKV) ReceiveReconfig(args *ReConfigArgs, reply *ReConfigReply) error {
	// update local database
	// args received from other replica groups by RPCs
	// args.Data, args.SeenOps and args.Previous should only have ata wrt. the shards that this GID owns
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("ReceiveReconfig: gid=%v, txId=%v, currconfig=%v, config=%v", kv.gid, args.TxId, kv.config.Num, args.Config.Num)

	if args.Config.Num <= kv.config.Num { // skip if already applied
		reply.Err = OK
		return nil
	}

	DPrintf("ReceiveReconfig: local db=%v", args.Data)
	for s, d := range args.Data {
		// ensure shard exists
		if _, e := kv.data[s]; !e {
			kv.data[s] = make(map[string]string)
		}
		for k, v := range d {
			kv.data[s][k] = v
		}
	}
	DPrintf("ReceiveReconfig: update seenOps=%v", args.SeenOps)
	// update local state
	for s, d := range args.SeenOps {
		if _, e := kv.seenOps[s]; !e {
			kv.seenOps[s] = make(map[int64]bool)
		}
		for k, v := range d {
			kv.seenOps[s][k] = v
		}
	}
	DPrintf("ReceiveReconfig: update previous=%v", args.Previous)
	// update previous values
	for s, d := range args.Previous {
		if _, e := kv.previous[s]; !e {
			kv.previous[s] = make(map[int64]string)
		}
		for k, v := range d {
			kv.previous[s][k] = v
		}
	}
	DPrintf("ReceiveReconfig: done")
	DPrintf("ReceiveReconfig: db=%v", kv.data)
	DPrintf("ReceiveReconfig: seenOps=%v", kv.seenOps)
	DPrintf("ReceiveReconfig: previous=%v", kv.previous)
	kv.config = kv.sm.Query(args.Config.Num)             // update local config
	kv.myShards = kv.GetShards(kv.config.Shards, kv.gid) // update my shards
	// kv.gid = args.Gid
	reply.Err = OK
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	config := kv.sm.Query(-1)
	// check if updated
	if config.Num > kv.config.Num {
		DPrintf("Tick: Reconfigure gid=%v, me=%v, config=%v", kv.gid, kv.me, config)
		kv.Reconfigure(config)
	}
	// fmt.Printf("Tick: config=%v\n", config)
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
	DPrintf("StartServer: me=%v, gid=%v\n", kv.me, kv.gid)

	kv.data = make(map[int]map[string]string)
	kv.seenOps = make(map[int]map[int64]bool)
	kv.previous = make(map[int]map[int64]string)
	kv.logSeq = -1
	kv.config = shardmaster.Config{} // local config
	kv.servers = servers
	kv.myServer = servers[kv.me]
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
