package shardmaster

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"sync"
	"syscall"
	"time"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs     []Config // indexed by config num
	gidToShards map[int64][]int64
	configNum   int
	logSeq      int
	shardNum    int
}

const (
	JOIN  = "JOIN"
	LEAVE = "LEAVE"
	MOVE  = "MOVE"
	QUERY = "QUERY"
)

type Op struct {
	// Your data here.
	Id   int64       // Id set for each Operation
	Seq  int         // Sequence Number
	Name string      // JOIN, LEAVE, MOVE, QUERY
	Args interface{} // JoinArgs, LeaveArgs, MoveArgs, QueryArgs
}

func (sm *ShardMaster) Wait(seq int) Op {
	to := 10 * time.Millisecond
	for { // keep trying
		if sm.dead {
			return Op{}
		}
		if ok, val := sm.px.Status(seq); ok {
			return val.(Op)
		}
		time.Sleep(to)
		// log.Printf("sleeping... seq=%v", seq)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

// start Paxos agreement with the Op
func (sm *ShardMaster) PaxosAgreement(op Op) string {
	// log.Printf("StartPaxosAgreement: me=%v, seq=%v, id=%v, clientId=%v", sm.me, sm.logSeq+1, op.Id, op.ClientId)
	for {
		seq := sm.logSeq + 1
		if sm.dead {
			return ""
		}
		var newOp Op
		// log.Printf("StartPaxos: calling status on seq=%v, me=%v", seq, sm.me)
		if decided, val := sm.px.Status(seq); decided {
			// decided
			newOp = val.(Op)
		} else {
			// not decided
			// log.Printf("StartPaxos: calling START on seq=%v, me=%v", seq, sm.me)
			op.Seq = seq
			sm.px.Start(seq, op) // call Start(); which will either discover the previously agreed-to-value or cause agreement to happen
			newOp = sm.Wait(seq)
		}

		log.Printf("StartPaxos: Decided id=%v, name=%v, seq=%v, me=%v", newOp.Id, newOp.Name, newOp.Seq, sm.me)
		// currValue := sm.data[newOp.Key] // if not exist, evalutes to "" (zero value of string type)
		// if newOp.Name == PUT {
		// 	sm.seenOps[newOp.Id] = true
		// 	sm.data[newOp.Key] = newOp.Val
		// } else if newOp.Name == PUTHASH {
		// 	sm.previous[newOp.ClientId] = currValue
		// 	sm.seenOps[newOp.Id] = true
		// 	sm.data[newOp.Key] = strconv.Itoa(int(hash(currValue + newOp.Val))) // hash
		// }

		sm.px.Done(seq) // this server is done with all instances <= seq
		sm.logSeq++     // try next seq number

		// if op.Id == newOp.Id { // decided value has same id as op
		// 	// we're up to date, so return
		// 	// for PutHash, this would equate to the previous value
		// 	// log.Printf("Server: Decided id=%v, seq=%v, val=%v", op.Id, seq, currValue)
		// 	return ""
		// }
	}
}

//
// Get argMin and argMax for the current replica set
//
func (sm *ShardMaster) MinMax(counts map[int64][]int64, groups map[int64][]string) (int64, int64) {
	min := 257
	var minShard int64
	max := 0
	var maxShard int64
	// log.Printf("MinMax: groups=%v\n", groups)
	for g, _ := range groups {
		if len(counts[g]) > max {
			max = len(counts[g])
			maxShard = g
		}
		if len(counts[g]) < min {
			min = len(counts[g])
			minShard = g
		}
	}
	return minShard, maxShard
}

//
// Update shards slice based on counts map
//
func (sm *ShardMaster) UpdateShards(counts map[int64][]int64, newShards [NShards]int64) [NShards]int64 {
	for GID, shards := range counts {
		for _, s := range shards {
			newShards[s] = GID
		}
	}
	return newShards
}

//
// Rebalance the NShards by moving max to min iteratively
// Return the new shard configuration
//
func (sm *ShardMaster) Rebalance(
	counts map[int64][]int64, // GID to list of shard numbers
	groups map[int64][]string,
	newShards [NShards]int64) [NShards]int64 {
	// get min/max of the new configuration
	minShard, maxShard := sm.MinMax(counts, groups)
	for len(counts[maxShard]) > len(counts[minShard])+1 {
		// move max to min
		temp := counts[maxShard][len(counts[maxShard])-1] // get last element
		// remove temp from maxShard
		// ref: https://stackoverflow.com/questions/34070369/removing-a-string-from-a-slice-in-go
		for i, v := range counts[maxShard] {
			if v == temp {
				counts[maxShard] = append(counts[maxShard][:i], counts[maxShard][i+1:]...)
			}
		}
		// append temp to min
		counts[minShard] = append(counts[minShard], temp)
		// recalculate min/max
		minShard, maxShard = sm.MinMax(counts, groups)
	}

	// update global shard counts
	sm.gidToShards = counts
	log.Printf("Rebalance: Done gidToShards=%v, newShards=%v", sm.gidToShards, newShards)
	return sm.UpdateShards(counts, newShards)
}

// The Join RPC’s arguments are a unique non-zero replica group identifier (GID)
// and an array of server ports. The shardmaster should react by creating a new
// configuration that includes the new replica group.
// The new configuration should divide the shards as evenly as possible among the groups,
// and should move as few shards as possible to achieve that goal.
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// GID: replica group identifier
	// Server: ports[]
	sm.mu.Lock()
	defer sm.mu.Unlock()
	log.Printf("Join: received with GID=%v, Servers=%v", args.GID, args.Servers)

	currConfig := sm.configs[len(sm.configs)-1] // current Config
	config := Config{}                          // create new Config

	// Config.Num
	config.Num = sm.configNum + 1
	sm.configNum++

	// Config.Groups
	// copy over all the current replica groups
	config.Groups = map[int64][]string{} // initialize
	for k, v := range currConfig.Groups {
		config.Groups[k] = v
	}
	// append new servers to GID group
	config.Groups[args.GID] = args.Servers

	// Config.Shards
	// determine how many shards in each replica
	counts := make(map[int64][]int64)
	counts[args.GID] = make([]int64, 0) // create empty k/v pair with the new GID
	for s, g := range currConfig.Shards {
		if g == 0 { // new shard; update
			counts[args.GID] = append(counts[args.GID], int64(s))
		} else { // existing shard; update count
			counts[g] = append(counts[g], int64(s))
		}
	}
	// pass in counts, new groups and old shard configuration
	config.Shards = sm.Rebalance(counts, config.Groups, currConfig.Shards)

	// apply change
	sm.configs = append(sm.configs, config)
	log.Printf("Join: New Config=%v", config)
	return nil
}

// The Leave RPC’s arguments are the GID of a previously joined group.
// The shardmaster should create a new configuration that does not include the group,
// and that assigns the group’s shards to the remaining groups. The new configuration
// should divide the shards as evenly as possible among the groups, and should move as
// few shards as possible to achieve that goal.
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	log.Printf("Leave: received with GID=%v, me=%v", args.GID, sm.me)

	currConfig := sm.configs[len(sm.configs)-1] // current Config
	config := Config{}                          // create new Config
	log.Printf("Leave: currConfig=%v", currConfig)
	// Config.Num
	config.Num = sm.configNum + 1
	sm.configNum++

	// Config.Groups
	// copy over all the current replica groups except the args.GID
	config.Groups = map[int64][]string{} // initialize

	for k, v := range currConfig.Groups {
		if k != args.GID {
			config.Groups[k] = v
		}
	}

	// Config.Shards
	// determine how many shards in previous replica group
	counts := make(map[int64][]int64)
	var temp int64
	// get a random GID to assign the removed replica shards to
	for _, g := range currConfig.Shards {
		if g != args.GID {
			temp = g
			break
		}
	}
	// update counts
	for s, g := range currConfig.Shards {
		if g != args.GID {
			counts[g] = append(counts[g], int64(s))
		} else {
			// assign the removed groups shards to someone else
			counts[temp] = append(counts[temp], int64(s))
		}
	}

	// pass in counts, new groups and old shard configuration
	config.Shards = sm.Rebalance(counts, config.Groups, currConfig.Shards)
	sm.configs = append(sm.configs, config)
	log.Printf("Leave: New Config=%v", config)
	return nil
}

// The Move RPC’s arguments are a shard number and a GID.
// The shardmaster should create a new configuration in which the shard is assigned to the group.
// The main purpose of Move is to allow us to test your software, but it might also be
// useful to fine-tune load balance if some shards are more popular than others or some
// replica groups are slower than others. A Join or Leave following a Move will likely
// un-do the Move, since Join and Leave re-balance.
func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	log.Printf("Move: received with GID=%v, Shard=%v", args.GID, args.Shard)
	currConfig := sm.configs[len(sm.configs)-1] // current Config
	config := Config{}                          // create new Config
	log.Printf("Move: currConfig=%v", currConfig)

	// Config.Num
	config.Num = sm.configNum + 1
	sm.configNum++

	// Config.Groups
	// copy over all the current replica groups except the args.GID
	config.Groups = map[int64][]string{} // initialize
	for k, v := range currConfig.Groups {
		config.Groups[k] = v
	}

	// Config.Shards
	// determine how many shards in each replica
	counts := make(map[int64][]int64)
	for s, g := range currConfig.Shards {
		if g == 0 || s == args.Shard { // MOVE
			counts[args.GID] = append(counts[args.GID], int64(s))
		} else { // existing shard; update count
			counts[g] = append(counts[g], int64(s))
		}
	}
	log.Printf("Move: counts=%v, config=%v", counts, config)

	// update the shards; no need to rebalance
	config.Shards = sm.UpdateShards(counts, currConfig.Shards)

	sm.configs = append(sm.configs, config)

	log.Printf("Move: New Config=%v", config)
	return nil
}

// The Query RPC’s argument is a configuration number. The shardmaster replies with the
// configuration that has that number. If the number is -1 or bigger than the biggest
// known configuration number, the shardmaster should reply with the latest configuration.
// The result of Query(-1) should reflect every Join, Leave, or Move that completed before
// the Query(-1) RPC was sent.
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	log.Printf("Query: received argnum=%v, me=%v", args.Num, sm.me)
	if args.Num == -1 || args.Num >= len(sm.configs) {
		//reply with latest configuration
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		// reply with config of index
		reply.Config = sm.configs[args.Num]
	}
	log.Printf("Query: returning config=%v, me=%v", reply.Config, sm.me)
	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.logSeq = -1
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	log.Printf("StartServer: %v", sm.configs)
	sm.gidToShards = make(map[int64][]int64)
	sm.configNum = 0
	sm.shardNum = 0
	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
