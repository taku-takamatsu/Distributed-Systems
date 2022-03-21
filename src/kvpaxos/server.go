package kvpaxos

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// "value" information that kvpaxos will use Paxos to agree on, for each request
	// agreed-on-values
	// maybe:
	// Id: Some unique identifier?
	// Seq Number
	// Name: GET, PUT, PUTHASH
	// Key
	// Value
	Id       int64
	ClientId int64
	Seq      int    // seq number
	Name     string // GET, PUT, PUTHASH
	Key      string
	Val      string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	data   map[string]string
	state  map[int64]*State // State maps ClientId to respective Op
	logSeq int              // keep track of local log sequence
}

//
// Hint: if one of your kvpaxos servers falls behind (i.e. did not participate in the agreement for some instance),
// it will later need to find out what (if anything) was agreed to.
// A reasonable way to to this is to call Start(), which will either discover the previously agreed-to value,
// or cause agreement to happen. Think about what value would be reasonable to pass to Start() in this situation.
//

func (kv *KVPaxos) StartPaxosAgreement(op Op) (int, bool) {
	// update local log/state from last recorded seq num
	// assign next available Paxos instance
	for {
		// from last seen seq number to most recent
		seq := kv.logSeq + 1
		if kv.dead {
			return 0, false
		}
		var newOp Op
		// log.Printf("StartPaxos: calling status on seq=%v, me=%v", seq, kv.me)
		if decided, val := kv.px.Status(seq); decided {
			// decided
			newOp = val.(Op)
		} else {
			// not decided
			// log.Printf("StartPaxos: calling START on seq=%v, me=%v", seq, kv.me)
			op.Seq = seq
			kv.px.Start(seq, op) // call Start(); which will either discover the previously agreed-to-value or cause agreement to happen
			to := 10 * time.Millisecond
			for { // keep trying
				if kv.dead {
					return 0, false
				}
				if ok, val := kv.px.Status(seq); ok {
					newOp = val.(Op)
					break
				}
				time.Sleep(to)
				// log.Printf("LOG: sleeping... seq=%v", seq)
				if to < 10*time.Second {
					to *= 2
				}
			}
		}

		// newOp should have latest decided value for current seq
		// once decided, update our local database

		// log.Printf("StartPaxos: Decided clientId=%v, id=%v, name=%v, seq=%v, me=%v", newOp.ClientId, newOp.Id, newOp.Name, newOp.Seq, kv.me)
		prevVal, prevExist := kv.data[newOp.Key] // if not exist, evalutes to "" (zero value of string type)
		if newOp.Name == PUT {
			kv.state[newOp.ClientId] = &State{newOp.Id, PUT, "", OK}
			kv.data[newOp.Key] = newOp.Val
		} else if newOp.Name == PUTHASH {
			value := strconv.Itoa(int(hash(prevVal + newOp.Val)))
			kv.state[newOp.ClientId] = &State{newOp.Id, PUTHASH, prevVal, OK}
			kv.data[newOp.Key] = value
		} else if newOp.Name == GET {
			if prevExist { // if value exists, we'll store that as the state to return
				kv.state[newOp.ClientId] = &State{newOp.Id, GET, prevVal, OK}
			} else {
				kv.state[newOp.ClientId] = &State{newOp.Id, GET, "", ErrNoKey}
			}
		}

		kv.px.Done(seq) // this kvserver is done with all instances <= seq
		kv.logSeq++     // try next seq number

		if op.Id == newOp.Id { // decided value has same id as op
			// we're up to date, so break
			return newOp.Seq, true
		}
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	log.Printf("Server: GET k=%v, id=%v, me=%v", args.Key, args.Id, kv.me)

	// at most once semantic
	// note: only one outstanding put/get for a given client
	// so if same client request and transaction id, it's a duplicate
	if prevState, exist := kv.state[args.ClientId]; exist && prevState.Id == args.Id {
		reply.Value = prevState.Value
		reply.Err = prevState.Err
		log.Printf("SERVER: Duplicate GET request id=%v, val=%v, err=%v", args.Id, reply.Value, reply.Err)
		return nil
	}

	// start Paxos agreement
	op := Op{args.Id, args.ClientId, 0, GET, args.Key, ""}

	_, ok := kv.StartPaxosAgreement(op) // returns the seq number that it agreed to
	if !ok {
		// could be from a paxos error - let client retry
		log.Printf("Server: GET StartPaxosAgreement not ok id=%v, me=%v", args.Id, kv.me)
		return nil
	}
	// log.Printf("Server: GET selected seq=%v, me=%v", seq, kv.me)
	reply.Value = kv.data[args.Key]
	reply.Err = OK

	log.Printf("Server: GET successful k=%v me=%v", args.Key, kv.me)
	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	log.Printf("Server: PUT k=%v, dohash=%v, id=%v, me=%v", args.Key, args.DoHash, args.Id, kv.me)

	// at most once semantic
	// note: only one outstanding put/get for a given client
	// so if same client request and transaction id, it's a duplicate
	if prevState, seen := kv.state[args.ClientId]; seen && prevState.Id == args.Id {
		reply.PreviousValue = prevState.Value
		reply.Err = prevState.Err
		log.Printf("SERVER: Duplicate PUT request id=%v, prev=%v, prevErr=%v", args.Id, reply.PreviousValue, reply.Err)
		return nil
	}

	//putHash
	var op Op
	if args.DoHash {
		op = Op{args.Id, args.ClientId, 0, PUTHASH, args.Key, args.Value}
	} else {
		op = Op{args.Id, args.ClientId, 0, PUT, args.Key, args.Value}
	}

	log.Printf("Server: PUT Starting Paxos Agreement me=%v", kv.me)
	// Agree on next available Paxos seq
	seq, ok := kv.StartPaxosAgreement(op)
	if !ok {
		// could be from a paxos error - let client retry
		log.Printf("Server: PUT StartPaxosAgreement not ok id=%v, me=%v", args.Id, kv.me)
		return nil
	}

	// return value from state
	// value should be decided by Paxos agreement round
	if val, exist := kv.state[args.ClientId]; exist {
		reply.Err = val.Err
		reply.PreviousValue = val.Value
	}
	log.Printf("Server: PUT successful k=%v, seq=%v, prev=%v, me=%v, id=%v", args.Key, seq, reply.PreviousValue, kv.me, args.Id)

	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.data = make(map[string]string)
	kv.state = make(map[int64]*State)
	kv.logSeq = -1

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
					fmt.Printf("KVPaxos: Discard me=%v\n", kv.me)
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					fmt.Printf("KVPaxos: Discard Reply me=%v\n", kv.me)
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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
