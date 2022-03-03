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
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
)

const (
	prepare = "prepare"
	accept  = "accept"
	decide  = "decide"
	REJECT  = "reject"
	OK      = "ok"
)

type HandlerArg struct {
	Req string // request type: either prepare, accept or decide
	Seq int
	N   int         // unique N
	V   interface{} // value
}

type HandlerReply struct {
	Res string      // either REJECT or OK
	N   int         // could be N or Na
	V   interface{} // V or Va, or nil
}

type PrepareOK struct {
	OK bool
	N  int
	V  interface{}
}

type AcceptOK struct {
	OK bool
	N  int
}
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
	Z         map[int]int   // Highest seq number passed to Done(), indexed by peer i
	maxRound  int           // largest N value seen so far
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
			log.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
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
	// Your code here.
	// start a proposer function in it's own thread for each instance, as needed (e.g. in Start()
	// Write a proposer function that drives the Paxos protocol for an instance, and RPC handlers that implement acceptors.

	// should immediately return without waiting for agreement to complete
	// use Status() to figure out whether Paxos peer thinks the instance has reached agreement, and if so, what the agreed value is.
	go func() {
		px.Proposer(seq, v)
	}()
}

func (px *Paxos) Proposer(seq int, v interface{}) error {
	log.Printf("Proposer: Called with seq=%v and v=%v; current min=%v, \n", seq, v, px.Min())
	// get the corresponding instance
	for decided, _ := px.Status(seq); !decided; { // while not decided:
		//  choose n, unique and higher than any n seen so far
		px.mu.Lock()
		px.maxRound++ // highest round number seen globally
		n := px.maxRound + px.me
		// px.instances[seq] = instance
		px.mu.Unlock()
		//  send prepare(n) to all servers including self
		log.Printf("Proposer: Send Prepare with n=%v and v=%v \n", n, v)
		prepareRes := px.SendPrepare(n, v, seq)
		if prepareRes.OK { // wait until majority of acceptors return PROPOSE-OK responses
			// log.Println("Proposer: Received Prepare-OK")
			//  if prepare_ok(n_a, v_a) from majority:
			//    v' = v_a with highest n_a; choose own v otherwise
			//    send accept(n, v') to all
			log.Printf("Proposer: Send Accept with n=%v and v=%v \n", n, prepareRes.V)
			acceptRes := px.SendAccept(n, prepareRes.V, seq)
			// log.Println("Proposer: received response from Accept:", acceptRes)

			//    if accept_ok(n) from majority:
			if acceptRes.OK {
				// send decided(v') to all
				log.Printf("Proposer: Send Decide with v=%v \n", prepareRes.V)
				px.SendDecide(prepareRes.V, seq) // no response, doesn't need to wait
			}
		}
		// retry decided arg
		decided, _ = px.Status(seq)
	}
	log.Println("Proposer: returned")
	return nil
}

// Prepare(N) --> RPC to all peers including self
// send RPC's to all peers
func (px *Paxos) SendPrepare(N int, v interface{}, seq int) PrepareOK {
	// log.Println("SendPrepare: Prepare received", N, v)
	// build args
	arg := &HandlerArg{prepare, seq, N, ""}
	acks := 0 // count of majority of acks received from acceptor
	nPeers := len(px.peers)
	highestN := 0
	var highestV interface{}
	for i, p := range px.peers {
		var reply HandlerReply
		log.Println("SendPrepare: Sending Prepare to Acceptor:", p)
		var ok bool
		if i == px.me {
			// in order to pass tests assuming unreliable network,
			// paxos should call the local acceptor through a function call rather than RPC.
			px.Acceptor(arg, &reply) //local method call if self
			ok = true                // shouldn't be any network loss
		} else {
			ok = call(p, "Paxos.Acceptor", arg, &reply)
		}
		log.Println("SendPrepare: Received Response from Acceptor:", ok, reply)
		if ok && reply.Res == OK {
			// if Prepare_OK, increment acks
			if highestN < reply.N { // Na
				highestN = reply.N // Na
				highestV = reply.V // Va
			}
			acks++
		}
		log.Printf("SendAccept: Determining majority %v/%v for seq=%v \n", acks, nPeers, seq)
		if acks > nPeers/2 { // if majority, return OK
			log.Println("SendPrepare: Majority received")
			// choose V = the value of the highest-numbered proposal among those returned by the acceptors
			// (or any value he wants if no Va was returned by any acceptor)
			if highestV == nil {
				highestN = N
				highestV = v // choose own if no Va
			}
			return PrepareOK{true, highestN, highestV}
		}
	}
	return PrepareOK{false, 0, nil}
}

func (px *Paxos) SendAccept(N int, V interface{}, seq int) AcceptOK {
	// build args
	arg := &HandlerArg{accept, seq, N, V}
	acks := 0 // count of majority of acks received from acceptor
	nPeers := len(px.peers)
	for i, p := range px.peers {
		var reply HandlerReply
		var ok bool
		if i == px.me {
			px.Acceptor(arg, &reply) //local method call if self
			ok = true
		} else {
			ok = call(p, "Paxos.Acceptor", arg, &reply)
		}
		if ok && reply.Res == OK {
			acks++ // if Prepare_OK, increment acks
		}
		log.Printf("SendAccept: Determining majority %v/%v for seq=%v \n", acks, nPeers, seq)
		if acks > nPeers/2 { // if majority, return OK
			log.Println("SendAccept: Majority received")
			return AcceptOK{true, N}
		}
	}
	return AcceptOK{false, N}
}

func (px *Paxos) SendDecide(Va interface{}, seq int) {
	// Send <DECIDE, Va> to all replicas
	arg := &HandlerArg{decide, seq, 0, Va} // 0 not used
	for i, p := range px.peers {
		var reply HandlerReply
		// Don't have to wait tio receive decide_ok (https://edstem.org/us/courses/19078/discussion/1215100)
		if i == px.me {
			px.Acceptor(arg, &reply) //local method call if self
		} else {
			call(p, "Paxos.Acceptor", arg, &reply)
		}
	}
}

func (px *Paxos) Acceptor(args *HandlerArg, reply *HandlerReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	log.Printf("Acceptor: Received req from proposer arg=%v\n", args)
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
		// close the protocol
		// nodes that might not have heard previous ACCEPT messages learn the chosen value
		instance.value = args.V
		px.instances[args.Seq] = instance
		px.Done(args.Seq)
		// don't bother updating Acceptor states, just send back OK (ref: https://edstem.org/us/courses/19078/discussion/1215100)
		reply.Res = OK
	}
	log.Printf("Acceptor: Returning reply to Proposer: %v", reply)
	return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// locked from parent
	log.Printf("Done: Called for seq=%v \n", seq)
	if seq > px.Z[px.me] {
		px.Z[px.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	// Your code here.
	m := math.MinInt32
	for _, z := range px.Z {
		if z > m {
			m = z
		}
	}
	log.Println("Max: ", m)
	return m
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
	// px.mu.Lock()
	// defer px.mu.Unlock()
	m := px.Z[px.me] // current me
	for _, z := range px.Z {
		if z < m {
			m = z
		}
	}
	return m + 1 // one more than min
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()
	// Status() should consult the local Paxos peer’s state and return immediately;
	// If Status() is called with a sequence number less than Min() -> return false
	if seq < px.Min() {
		log.Printf("Status: seq=%v too low than min=%v\n", seq, px.Min())
		return false, nil
	}
	v, exist := px.instances[seq]
	if exist && v.value != nil { // any decided instance for this instance will have a value
		log.Printf("Status: seq=%v for px=%v found value=%v\n", seq, px.me, v.value)
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
	px.maxRound = 0
	px.instances = make(map[int]State)
	px.Z = make(map[int]int)
	for p := range px.peers { // pnitialize with -1 (done() never called)
		px.Z[p] = -1
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
