package paxos

import (
	"coms4113/hw5/pkg/base"
)

const (
	Propose = "propose"
	Accept  = "accept"
	Decide  = "decide"
)

type Proposer struct {
	N             int
	Phase         string
	N_a_max       int
	V             interface{}
	SuccessCount  int
	ResponseCount int
	// To indicate if response from peer is received, should be initialized as []bool of len(server.peers)
	Responses []bool
	// Use this field to check if a message is latest.
	SessionId int

	// in case node will propose again - restore initial value
	InitialValue interface{}
}

type ServerAttribute struct {
	peers []base.Address
	me    int

	// Paxos parameter
	n_p int
	n_a int
	v_a interface{}

	// final result
	agreedValue interface{}

	// Propose parameter
	proposer Proposer

	// retry
	timeout *TimeoutTimer
}

type Server struct {
	base.CoreNode
	ServerAttribute
}

func NewServer(peers []base.Address, me int, proposedValue interface{}) *Server {
	response := make([]bool, len(peers))
	return &Server{
		CoreNode: base.CoreNode{},
		ServerAttribute: ServerAttribute{
			peers: peers,
			me:    me,
			proposer: Proposer{
				InitialValue: proposedValue,
				Responses:    response,
			},
			timeout: &TimeoutTimer{},
		},
	}
}

func (server *Server) MessageHandler(message base.Message) []base.Node {
	//TODO: implement it
	newNodes := make([]base.Node, 0)
	switch message.(type) {
	case *ProposeRequest: // <PROPOSE, N> request
		// maintains some state:
		// Na: highest accepted proposal
		// Va: value of highest accepted proposal
		// Np: highest proposal number they've seen to date
		msg := message.(*ProposeRequest)
		if msg.N > server.n_p {
			// update Np = N
			// return <PROPOSE-OK, Na, Va>
			newNode := server.copy() // copy current node
			newNode.n_p = msg.N
			response := &ProposeResponse{
				CoreMessage: base.MakeCoreMessage(message.From(), message.To()),
				Ok:          true, N_p: newNode.n_p, V_a: newNode.v_a}
			newNode.SetSingleResponse(response)
			newNodes = append(newNodes, newNode)
		} else {
			// return <REJECT, Np>
			newNode := server.copy() // copy current node
			response := &ProposeResponse{
				CoreMessage: base.MakeCoreMessage(message.From(), message.To()),
				Ok:          false, N_p: newNode.n_p}
			newNode.SetSingleResponse(response)
			newNodes = append(newNodes, newNode)
		}
	case *AcceptRequest: // <ACCEPT, N, V>
		msg := message.(*AcceptRequest)
		if msg.N >= server.n_p {
			// update Np=N, Na=N, Va=V
			newNode := server.copy()
			newNode.n_p = msg.N
			newNode.n_a = msg.N
			newNode.v_a = msg.V
			response := &AcceptResponse{
				CoreMessage: base.MakeCoreMessage(message.From(), message.To()),
				Ok:          true, N_p: msg.N}
			newNode.SetSingleResponse(response)
			newNodes = append(newNodes, newNode)
		} else {
			// <REJECT, Np>
			newNode := server.copy()
			response := &AcceptResponse{
				CoreMessage: base.MakeCoreMessage(message.From(), message.To()),
				Ok:          false, N_p: newNode.n_p}
			newNode.SetSingleResponse(response)
			newNodes = append(newNodes, newNode)
		}
	case *DecideRequest:
		msg := message.(*DecideRequest)
		newNode := server.copy()
		newNode.agreedValue = msg.V
		newNodes = append(newNodes, newNode)

	case *ProposeResponse:
		// handle ProposeResponse;
		msg := message.(*ProposeResponse)
		// first scenario: proposer receives majority OK
		// determine highest-numbered proposal value

		if msg.Ok { // if <PROPOSE, OK>
			// Send <ACCEPT, N, V>
			newNode := server.copy()
			newNode.proposer.Phase = Accept
			responses := make([]base.Message, 0)
			for _, peer := range newNode.peers {
				response := &AcceptRequest{
					CoreMessage: base.MakeCoreMessage(message.From(), peer),
					N:           msg.N_p,
					V:           newNode.proposer.V,
					SessionId:   newNode.proposer.SessionId} // choose V; highest-numbered proposal among those returned
				responses = append(responses, response)
			}
			newNode.SetResponse(responses)
			newNodes = append(newNodes, newNode)
		}
		// second scenario: waiting for rest of the responses
		newNode := server.copy()
		newNode.proposer.Phase = Propose
		for i := 0; i < len(newNode.peers); i++ {
			if msg.To() == newNode.peers[i] || msg.From() == newNode.peers[i] {
				newNode.proposer.Responses[i] = true
				newNode.proposer.SuccessCount++
				newNode.proposer.ResponseCount++
			}
		}
		newNodes = append(newNodes, newNode)
	case *AcceptResponse: // handle AcceptResponse
		msg := message.(*AcceptResponse)
		// Scenario 1: ACCEPT-OK received from majority; prepare DECIDE
		if msg.Ok {
			newNode := server.copy()
			newNode.proposer.Phase = Decide
			// set decided value

			newNode.n_p = msg.N_p
			//newNode.proposer.N_a_max = newNode.n_a
			responses := make([]base.Message, 0)
			for _, peer := range newNode.peers {
				response := &DecideRequest{
					CoreMessage: base.MakeCoreMessage(message.From(), peer),
					V:           newNode.proposer.V,
					SessionId:   newNode.proposer.SessionId} // choose V; highest-numbered proposal among those returned
				responses = append(responses, response)
			}
			newNode.SetResponse(responses)
			newNodes = append(newNodes, newNode)
		}
		// second scenario: waiting for rest of the responses
		newNode := server.copy()
		newNode.proposer.Phase = Accept

		for i := 0; i < len(newNode.peers); i++ {
			if msg.To() == newNode.peers[i] || msg.From() == newNode.peers[i] {
				// majority received, still waiting for others
				newNode.proposer.Responses[i] = true
				newNode.proposer.SuccessCount++
				newNode.proposer.ResponseCount++
			}
		}
		newNodes = append(newNodes, newNode)

	}
	return newNodes
}

// To start a new round of Paxos.
func (server *Server) StartPropose() {
	//TODO: implement it
	// renew proposer's fields
	server.proposer.N++
	server.proposer.SessionId++
	server.proposer.Phase = Propose
	server.proposer.V = server.proposer.InitialValue
	// then send ProposeRequest to all of it's peers

	responses := make([]base.Message, 0)
	from := server.peers[server.me]
	for _, peer := range server.peers {
		req := &ProposeRequest{
			CoreMessage: base.MakeCoreMessage(from, peer),
			N:           server.proposer.N,
			SessionId:   server.proposer.SessionId}
		responses = append(responses, req)
	}
	server.SetResponse(responses)
}

// Returns a deep copy of server node
func (server *Server) copy() *Server {
	response := make([]bool, len(server.peers))
	for i, flag := range server.proposer.Responses {
		response[i] = flag
	}

	var copyServer Server
	copyServer.me = server.me
	// shallow copy is enough, assuming it won't change
	copyServer.peers = server.peers
	copyServer.n_a = server.n_a
	copyServer.n_p = server.n_p
	copyServer.v_a = server.v_a
	copyServer.agreedValue = server.agreedValue
	copyServer.proposer = Proposer{
		N:             server.proposer.N,
		Phase:         server.proposer.Phase,
		N_a_max:       server.proposer.N_a_max,
		V:             server.proposer.V,
		SuccessCount:  server.proposer.SuccessCount,
		ResponseCount: server.proposer.ResponseCount,
		Responses:     response,
		InitialValue:  server.proposer.InitialValue,
		SessionId:     server.proposer.SessionId,
	}

	// doesn't matter, timeout timer is state-less
	copyServer.timeout = server.timeout

	return &copyServer
}

func (server *Server) NextTimer() base.Timer {
	return server.timeout
}

// A TimeoutTimer tick simulates the situation where a proposal procedure times out.
// It will close the current Paxos round and start a new one if no consensus reached so far,
// i.e. the server after timer tick will reset and restart from the first phase if Paxos not decided.
// The timer will not be activated if an agreed value is set.
func (server *Server) TriggerTimer() []base.Node {
	if server.timeout == nil {
		return nil
	}

	subNode := server.copy()
	subNode.StartPropose()

	return []base.Node{subNode}
}

func (server *Server) Attribute() interface{} {
	return server.ServerAttribute
}

func (server *Server) Copy() base.Node {
	return server.copy()
}

func (server *Server) Hash() uint64 {
	return base.Hash("paxos", server.ServerAttribute)
}

func (server *Server) Equals(other base.Node) bool {
	otherServer, ok := other.(*Server)

	if !ok || server.me != otherServer.me ||
		server.n_p != otherServer.n_p || server.n_a != otherServer.n_a || server.v_a != otherServer.v_a ||
		(server.timeout == nil) != (otherServer.timeout == nil) {
		return false
	}

	if server.proposer.N != otherServer.proposer.N || server.proposer.V != otherServer.proposer.V ||
		server.proposer.N_a_max != otherServer.proposer.N_a_max || server.proposer.Phase != otherServer.proposer.Phase ||
		server.proposer.InitialValue != otherServer.proposer.InitialValue ||
		server.proposer.SuccessCount != otherServer.proposer.SuccessCount ||
		server.proposer.ResponseCount != otherServer.proposer.ResponseCount {
		return false
	}

	for i, response := range server.proposer.Responses {
		if response != otherServer.proposer.Responses[i] {
			return false
		}
	}

	return true
}

func (server *Server) Address() base.Address {
	return server.peers[server.me]
}
