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
	Id       int64  // Id set for each Operation
	ClientId int64  // Id set for Clerk
	Seq      int    // seq number
	Name     string // GET, PUT, PUTHASH
	Key      string
	Val      string
}

type State struct {
	Id  int64
	Seq int
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	data     map[string]string
	previous map[int64]string // keep track of previous value for PutHash
	// seenOps: stores all op Id's for at-most-once semantic
	// we need this as kvpaxos essentially has no idea if client(s) are done with the current op
	// ref: https://edstem.org/us/courses/19078/discussion/1258568
	seenOps map[int64]bool
	logSeq  int // keep track of local log sequence
}

//
// Hint: if one of your kvpaxos servers falls behind (i.e. did not participate in the agreement for some instance),
// it will later need to find out what (if anything) was agreed to.
// A reasonable way to to this is to call Start(), which will either discover the previously agreed-to value,
// or cause agreement to happen. Think about what value would be reasonable to pass to Start() in this situation.
//

func (kv *KVPaxos) Wait(seq int) Op {
	to := 10 * time.Millisecond
	for { // keep trying
		if kv.dead {
			return Op{}
		}
		if ok, val := kv.px.Status(seq); ok {
			return val.(Op)
		}
		time.Sleep(to)
		// log.Printf("sleeping... seq=%v", seq)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (kv *KVPaxos) StartPaxosAgreement(op Op) (string, bool) {
	// log.Printf("StartPaxosAgreement: me=%v, seq=%v, id=%v, clientId=%v", kv.me, kv.logSeq+1, op.Id, op.ClientId)
	// update local log/state from last recorded seq num
	// loop from last seen seq number to next available Paxos instance
	for {
		seq := kv.logSeq + 1
		if kv.dead {
			return "", false
		}
		// wait for this seq is decided
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
			newOp = kv.Wait(seq)
		}

		// newOp should have latest decided value for current seq
		// update our local db/state
		// log.Printf("StartPaxos: Decided clientId=%v, id=%v, name=%v, seq=%v, me=%v", newOp.Id, newOp.Id, newOp.Name, newOp.Seq, kv.me)
		currValue := kv.data[newOp.Key] // if not exist, evalutes to "" (zero value of string type)
		if newOp.Name == PUT {
			kv.seenOps[newOp.Id] = true
			kv.data[newOp.Key] = newOp.Val
		} else if newOp.Name == PUTHASH {
			kv.previous[newOp.ClientId] = currValue
			kv.seenOps[newOp.Id] = true
			kv.data[newOp.Key] = strconv.Itoa(int(hash(currValue + newOp.Val))) // hash
		}

		kv.px.Done(seq) // this kvserver is done with all instances <= seq
		kv.logSeq++     // try next seq number

		if op.Id == newOp.Id { // decided value has same id as op
			// we're up to date, so return
			// for PutHash, this would equate to the previous value
			// log.Printf("Server: Decided id=%v, seq=%v, val=%v", op.Id, seq, currValue)
			return currValue, true
		}
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// We don't need to cache Get operations as the client can just simply retry
	// Ref: https://edstem.org/us/courses/19078/discussion/1258568
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// log.Printf("Server: GET k=%v, id=%v, me=%v", args.Key, args.Id, kv.me)
	// start Paxos agreement
	op := Op{args.Id, args.ClientId, 0, GET, args.Key, ""}
	// log.Printf("Server: GET Starting Paxos Agreement me=%v from seq=%v", kv.me, kv.logSeq+1)
	value, ok := kv.StartPaxosAgreement(op) // returns the seq number that it agreed to
	if !ok {
		return nil // maybe Paxos error? - let client retry
	}
	// log.Printf("Server: GET selected seq=%v, me=%v", seq, kv.me)
	reply.Value = value
	reply.Err = OK

	// log.Printf("Server: GET successful k=%v me=%v", args.Key, kv.me)
	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// log.Printf("Server: PUT k=%v, dohash=%v, id=%v, me=%v", args.Key, args.DoHash, args.Id, kv.me)

	// at most once semantic
	// note: only one outstanding put/get for a given client
	// so if same client request and transaction id, it's a duplicate
	//
	// If a reply is dropped and another server receives the same op,
	// may not detect a duplicate here but it should through the Paxos rounds
	// ref: https://edstem.org/us/courses/19078/discussion/1300037?comment=2950718
	if _, exist := kv.seenOps[args.Id]; exist {
		if args.DoHash {
			reply.PreviousValue = kv.previous[args.ClientId]
		}
		reply.Err = OK
		// log.Printf("SERVER: Duplicate PUT request id=%v, prevErr=%v", args.Id, reply.Err)
		return nil
	}

	//putHash
	var op Op
	if args.DoHash {
		op = Op{args.Id, args.ClientId, 0, PUTHASH, args.Key, args.Value}
	} else {
		op = Op{args.Id, args.ClientId, 0, PUT, args.Key, args.Value}
	}

	// log.Printf("Server: PUT Starting Paxos Agreement me=%v from seq=%v", kv.me, kv.logSeq+1)
	// Start Paxos agreement
	value, ok := kv.StartPaxosAgreement(op) // returns previous value (if PutHash)
	if !ok {
		return nil // maybe Paxos error? - let client retry
	}

	reply.Err = OK
	reply.PreviousValue = value // value != "" if PutHash

	// log.Printf("Server: PUT successful k=%v, v=%v, me=%v, id=%v, clientId=%v", args.Key, value, kv.me, args.Id, args.ClientId)
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
	kv.previous = make(map[int64]string)
	kv.seenOps = make(map[int64]bool)
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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
