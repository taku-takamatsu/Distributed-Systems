package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"time"
)

const (
	prepare = "prepare"
	accept  = "accept"
	decide  = "decide"
	REJECT  = "reject"
	OK      = "ok"
)

// RPCs -- remember CAPS!
type HandlerArg struct {
	Req  string // request type: either prepare, accept or decide
	Seq  int
	N    int         // unique N
	V    interface{} // value
	From int
	Done int // done seq number
}
type HandlerReply struct {
	Res  string      // either REJECT or OK
	N    int         // could be N or Na
	V    interface{} // V or Va, or nil
	From int
	Done int // done seq number
}

// helpers
type PrepareOK struct {
	OK bool
	N  int
	V  interface{}
}
type AcceptOK struct {
	OK bool
	N  int
}

// State of each instance
// holds Acceptor state + actual value when decided
type State struct {
	Na    int         // highest accepted proposal
	Va    interface{} // value of the highest accepted proposal
	Np    int         // highest proposal number i've seen to date
	value interface{} // decided value (could be empty)
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.

	// Each instance should have its own separate execution of the Paxos protocol (able to execute instances concurrently)
	instances map[int]State // corresponding instance + state, indexed by seq
	done      map[int]int   // = Z; Highest seq number passed to Done(), indexed by peer i
	propNum   int           // largest N value seen so far
	maxSeq    int           // keep track of highest instance sequence known to peer
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			// log.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	// log.Println("Paxos:", err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// If Start() is called with a sequence number less than Min(), the Start() call should be ignored.
	if seq < px.Min() {
		return
	}
	// Start a proposer function in it's own thread for each instance, as needed (e.g. in Start()
	go func() {
		px.Proposer(seq, v)
	}() // to return immediately
}

func (px *Paxos) Proposer(seq int, v interface{}) error {
	// log.Printf("Proposer: Called with seq=%v, current min=%v, \n", seq, px.Min())
	// get the corresponding instance
	px.mu.Lock()
	px.propNum = -1 // first round will be 0
	px.mu.Unlock()
	decided := false
	for !decided && !px.dead { // while not decided and proposer not dead:
		//  choose n, unique and higher than any n seen so far
		px.mu.Lock()
		px.propNum++ // highest round number seen globally
		// Note: highest number seen for that instance
		n := px.propNum + px.me
		px.mu.Unlock()
		//  send prepare(n) to all servers including self
		// log.Printf("Proposer: Send Prepare with propNum=%v, n=%v, seq=%v \n", px.propNum, n, seq)
		prepareRes := px.SendPrepare(n, v, seq)
		if prepareRes.OK { // wait until majority of acceptors return PROPOSE-OK responses
			//  if prepare_ok(n_a, v_a) from majority:
			//    v' = v_a with highest n_a; choose own v otherwise
			//    send accept(n, v') to all
			// log.Printf("Proposer: Send Accept with n=%v, seq=%v \n", n, seq)
			acceptRes := px.SendAccept(n, prepareRes.V, seq)
			//    if accept_ok(n) from majority:
			if acceptRes.OK {
				// send decided(v') to all
				// log.Printf("Proposer: Send Decide seq=%v\n", seq)
				px.SendDecide(acceptRes.N, prepareRes.V, seq) // no response, doesn't need to wait
				decided = true

			} else {
<<<<<<< HEAD
				// if reject => acceptor responds with Np
				// use that to propose the next round
=======
>>>>>>> a3p2/in-prog
				px.mu.Lock()
				// log.Printf("Proposer: Accept rejected Np=%v", acceptRes.N)
				if n < acceptRes.N {
					px.propNum = acceptRes.N
				}
				px.mu.Unlock()
			}
<<<<<<< HEAD
		} else {
			// if reject => acceptor responds with Np
			// use that to propose the next round
=======
			// if reject => acceptor responds with Np
			// use that to propose the next round
		} else {
>>>>>>> a3p2/in-prog
			px.mu.Lock()
			// log.Printf("Proposer: Prepare rejected Np=%v", prepareRes.N)
			if n < prepareRes.N {
				px.propNum = prepareRes.N
			}
			px.mu.Unlock()
		}
		if !decided {
			// if not decided, we'll retry with random backoff
			// log.Printf("Paxos: random backoff for me=%v, seq=%v", px.me, seq)
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		}
	}
	// log.Printf("Proposer DONE me=%v, seq=%v", px.me, seq)
	return nil
}

func (px *Paxos) PiggyBack(from int, done int) {
	// from: peer
	// done: done value from peer
	px.mu.Lock()
	if px.done[from] < done {
		px.done[from] = done
	}
	// log.Printf("PiggyBack: me=%v, from=%v, done=%v, px.done=%v", px.me, from, done, px.done)
	px.mu.Unlock()
}

func (px *Paxos) GetDone() int {
	px.mu.Lock()
	done, exist := px.done[px.me]
	px.mu.Unlock()
	if exist {
		return done
	} else {
		return -1
	}
}

// Prepare(N) --> RPC to all peers including self
func (px *Paxos) SendPrepare(N int, v interface{}, seq int) PrepareOK {
	// build args
	arg := &HandlerArg{prepare, seq, N, "", px.me, -1}
	acks := 0 // count of majority of acks received from acceptor
	nPeers := len(px.peers)
	highestNa := 0
	rejectNp := 0
	var highestVa interface{}
	for i, p := range px.peers {
		arg.Done = px.GetDone() // update done value
		var reply HandlerReply
		// log.Println("SendPrepare: Sending Prepare to Acceptor:", p)
		var ok bool
		if i == px.me {
			// in order to pass tests assuming unreliable network,
			// paxos should call the local acceptor through a function call rather than RPC.
			px.Acceptor(arg, &reply) //local method call if self
			ok = true                // shouldn't be any network loss
		} else {
			ok = call(p, "Paxos.Acceptor", arg, &reply)
		}
		if ok && reply.Res == OK {
			// log.Printf("SendPrepare: returned from=%v with done=%v\n", reply.From, reply.Done)
			px.PiggyBack(reply.From, reply.Done) // piggyback the done value
			// if Prepare_OK, increment acks
			if highestNa < reply.N { // Na
				highestNa = reply.N // Na
				highestVa = reply.V // Va
			}
			acks++
		} else {
			rejectNp = reply.N
		}
		// log.Printf("SendAccept: Determining majority %v/%v for seq=%v \n", acks, nPeers, seq)
		if acks > nPeers/2 { // if majority, return OK
			// log.Println("SendPrepare: Majority received")
			// choose V = the value of the highest-numbered proposal among those returned by the acceptors
			// (or any value he wants if no Va was returned by any acceptor)
			if highestVa == nil {
				highestNa = N
				highestVa = v // choose own if no Va
			}
			return PrepareOK{true, highestNa, highestVa}
		}
	}
	// log.Printf("SendPrepare: ERROR couldn't get majority")
	return PrepareOK{false, rejectNp, nil}
}

func (px *Paxos) SendAccept(N int, V interface{}, seq int) AcceptOK {
	// build args
	arg := &HandlerArg{accept, seq, N, V, px.me, -1}
	acks := 0 // count of majority of acks received from acceptor
	nPeers := len(px.peers)
	for i, p := range px.peers {
		arg.Done = px.GetDone() // update done value
		var reply HandlerReply
		var ok bool
		if i == px.me {
			px.Acceptor(arg, &reply) //local method call if self
			ok = true
		} else {
			ok = call(p, "Paxos.Acceptor", arg, &reply)
		}
		if ok && reply.Res == OK {
			// log.Printf("SendAccept: returned from=%v with done=%v", reply.From, reply.Done)
			px.PiggyBack(reply.From, reply.Done) // piggyback the done value
			acks++                               // if Prepare_OK, increment acks
		}
		// log.Printf("SendAccept: Determining majority %v/%v for seq=%v \n", acks, nPeers, seq)
		if acks > nPeers/2 { // if majority, return OK
			return AcceptOK{true, N}
		}
	}
	// log.Printf("SendAccept: ERROR couldn't get majority")
	return AcceptOK{false, N}
}

func (px *Paxos) SendDecide(N int, Va interface{}, seq int) {
	// log.Printf("SendDecide: Received request from seq=%v", seq)
	// Send <DECIDE, Va> to all replicas
	arg := &HandlerArg{decide, seq, N, Va, px.me, -1} // 0 not used
	for i, p := range px.peers {
		// log.Printf("SendDecide: Spawning go thread for seq=%v with i=%v, p=%v, me=%v\n", seq, i, p, px.me)
		go func(i int, p string) {
			arg.Done = px.GetDone() // update done value
			var reply HandlerReply
			var ok bool
			// Don't have to wait to receive decide_ok (https://edstem.org/us/courses/19078/discussion/1215100)
			if i == px.me {
				// log.Printf("SendDecide: making call to acceptor seq=%v, me=%v LOCAL\n", seq, px.me)
				px.Acceptor(arg, &reply) //local method call if self
				ok = true
			} else {
				// log.Printf("SendDecide: making call to acceptor seq=%v, me=%v RPC\n", seq, px.me)
				ok = call(p, "Paxos.Acceptor", arg, &reply)
			}
			if ok && reply.Res == OK {
				// log.Printf("SendDecide: reply OK, returning; for seq=%v, me=%v, i=%v, p=%v, from=%v", seq, px.me, i, p, reply.From)
				px.PiggyBack(reply.From, reply.Done) // piggyback the done value
				return
			} else {
				// log.Printf("SendDecide: reply NOT OK reply=%v; for seq=%v, me=%v, i=%v, p=%v", reply, seq, px.me, i, p)
			}
		}(i, p)
	}
}

func (px *Paxos) Acceptor(args *HandlerArg, reply *HandlerReply) error {
	// log.Printf("Acceptor: Received req from proposer arg=%v\n", args)
	// each paxos peer should tell each other peer the highest Done argument supplied by local app
	px.PiggyBack(args.From, args.Done) // piggyback
	px.mu.Lock()
	defer px.mu.Unlock()
	reply.From = px.me
	if done, exist := px.done[px.me]; exist { // send done value back to proposer
		reply.Done = done
	} else {
		reply.Done = -1
	}
	// current instance
	instance := px.instances[args.Seq]
	if args.Req == prepare { // PHASE 1
		if args.N > instance.Np { // receive <PROPOSE, N>
			instance.Np = args.N // update Np
			px.instances[args.Seq] = instance
			reply.Res = OK
			reply.N = instance.Na
			reply.V = instance.Va
		} else {
			reply.Res = REJECT
			reply.N = instance.Np // <REJECT, NP>
		}
	} else if args.Req == accept { // PHASE 2
		if args.N >= instance.Np { // receive <ACCEPT, N, V>
			//update Np = N, Na = N, Va = V
			instance.Np = args.N
			instance.Na = args.N
			instance.Va = args.V
			px.instances[args.Seq] = instance
			reply.Res = OK
			reply.N = args.N
		} else {
			reply.Res = REJECT // <REJECT, NP>
			reply.N = instance.Np
		}
	} else if args.Req == decide { // Phase 3
		// don't bother updating Acceptor states, just send back OK
		// ref: https://edstem.org/us/courses/19078/discussion/1215100
		// jk lecture slides said to update
		if args.N >= instance.Np {
			instance.Np = args.N
			instance.Na = args.N
			instance.Va = args.V
		}
		// also update maximum sequence
		if px.maxSeq < args.Seq {
			px.maxSeq = args.Seq
		}
		instance.value = args.V
		px.instances[args.Seq] = instance
		reply.Res = OK
	}
	// log.Printf("Acceptor: me=%v reply to %v with done=%v", px.me, args.From, reply.Done)
	return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	if seq > px.done[px.me] { // px.done stores highest Done() sequence for each peer
		px.done[px.me] = seq
	}
	// log.Printf("Done: Called for seq=%v, me=%v, px.done=%v\n", seq, px.me, px.done)
	px.mu.Unlock()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	px.mu.Lock()
	m := px.done[px.me] // min can only be lower than current done()
	// log.Printf("Min: me=%v, minVal=%v,  px.done=%v", px.me, m, px.done)
	for _, z := range px.done {
		if z < m {
			m = z
		}
	}
	px.mu.Unlock()
	px.Forget(m) // forget all done instances below min()
	return m + 1 // one more than min
}

func (px *Paxos) Forget(min int) {
	// given min value, each peer should discard instances <= min()
	px.mu.Lock()
	defer px.mu.Unlock()
	// var m0 runtime.MemStats
	// runtime.ReadMemStats(&m0)
	for seq, _ := range px.instances {
		if seq <= min {
			delete(px.instances, seq)
		}
	}
	// var m1 runtime.MemStats
	// runtime.ReadMemStats(&m1)
	// if m0.Alloc != m1.Alloc {
	// 	log.Printf("Paxos: Forget Memory before=%v after=%v, min=%v", m0.Alloc, m1.Alloc, min)
	// }
	// log.Printf("Forget: me=%v, px.done=%v", px.me, px.done)
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Status() should consult the local Paxos peer’s state and return immediately;
	// If Status() is called with a sequence number less than Min() -> return false
	// log.Printf("Status: called with px.done=%v, seq=%v, me=%v", px.done, seq, px.me)
	if seq < px.Min() { //min thread locked inside function
		// log.Printf("Status: seq=%v too low than min=%v\n", seq, px.Min())
		return false, nil
	}
	px.mu.Lock() // lock for the rest
	defer px.mu.Unlock()
	v, exist := px.instances[seq]
	if exist && v.value != nil { // any decided instance for this peer will have a value
		// log.Printf("Paxos; Status: seq=%v for px=%v decided=%v\n", seq, px.me, px.done)
		return true, v.value
	}
	return false, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	// Your initialization code here.
	px.propNum = 0
	px.instances = make(map[int]State)
	px.done = make(map[int]int)
	for p := range px.peers { // initialize with -1 for each peer (done() never called)
		px.done[p] = -1
	}
	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							log.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					log.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
