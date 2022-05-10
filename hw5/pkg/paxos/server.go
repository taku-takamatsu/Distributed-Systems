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
				CoreMessage: base.MakeCoreMessage(message.To(), message.From()),
				Ok:          true, N_a: newNode.n_a, V_a: newNode.v_a, N_p: newNode.n_p,
				SessionId: msg.SessionId}
			newNode.SetSingleResponse(response)
			//newNode.proposer.SessionId = msg.SessionId
			newNodes = append(newNodes, newNode)
		} else {
			// return <REJECT, Np>
			newNode := server.copy() // copy current node
			response := &ProposeResponse{
				CoreMessage: base.MakeCoreMessage(message.To(), message.From()),
				Ok:          false, N_p: newNode.n_p,
				SessionId: msg.SessionId}
			newNode.SetSingleResponse(response)
			//newNode.proposer.SessionId = msg.SessionId
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
				CoreMessage: base.MakeCoreMessage(message.To(), message.From()),
				Ok:          true, N_p: msg.N,
				SessionId: msg.SessionId}
			newNode.SetSingleResponse(response)
			newNode.proposer.SessionId = msg.SessionId
			newNodes = append(newNodes, newNode)
		} else {
			// <REJECT, Np>
			newNode := server.copy()
			response := &AcceptResponse{
				CoreMessage: base.MakeCoreMessage(message.To(), message.From()),
				Ok:          false, N_p: newNode.n_p,
				SessionId: msg.SessionId}
			newNode.SetSingleResponse(response)
			newNode.proposer.SessionId = msg.SessionId
			newNodes = append(newNodes, newNode)
		}
	case *DecideRequest:
		msg := message.(*DecideRequest)
		newNode := server.copy()
		newNode.agreedValue = msg.V
		newNode.v_a = msg.V
		newNode.n_a = newNode.proposer.N
		newNodes = append(newNodes, newNode)

	case *ProposeResponse:
		// handle ProposeResponse;
		msg := message.(*ProposeResponse)
		// make sure msg session id matches proposer's session id
		if msg.SessionId == server.proposer.SessionId {
			if msg.Ok { // if <PROPOSE, OK>
				// Send <ACCEPT, N, V>
				// see if we've reached majority
				successCount := server.proposer.SuccessCount
				for i := 0; i < len(server.peers); i++ {
					// make sure message not from peer already seen
					if msg.From() == server.peers[i] && server.proposer.Responses[i] == false {
						successCount++ // increment success count
					}
				}
				// we can move onto Accept phase if we've reached majority
				if successCount >= (len(server.peers)/2)+1 {
					// Scenario 1: majority reached
					newNode := server.copy()
					newNode.proposer.Phase = Accept

					// choose V; value of highest-numbered proposal among those returned
					if base.IsNil(msg.V_a) && base.IsNil(newNode.proposer.V) {
						newNode.proposer.V = newNode.proposer.InitialValue
					} else if !base.IsNil(msg.V_a) && msg.N_a > newNode.proposer.N_a_max {
						newNode.proposer.N_a_max = msg.N_a
						newNode.proposer.V = msg.V_a
					}

					responses := make([]base.Message, 0)
					for _, peer := range newNode.peers {
						response := &AcceptRequest{
							CoreMessage: base.MakeCoreMessage(newNode.Address(), peer),
							N:           newNode.proposer.N,
							V:           newNode.proposer.V,
							SessionId:   msg.SessionId} // choose V; highest-numbered proposal among those returned
						responses = append(responses, response)
					}
					newNode.SetResponse(responses)
					// reset response and success count; as we move onto next phase
					newNode.proposer.ResponseCount = 0
					newNode.proposer.SuccessCount = 0
					newNode.proposer.Responses = make([]bool, len(newNode.peers))
					newNodes = append(newNodes, newNode)
				}
				// Scenario 2: If successCount not majority or haven't received all responses
				if server.proposer.ResponseCount+1 < len(server.peers) && successCount < len(server.peers) {
					newNode := server.copy()
					newNode.proposer.Phase = Propose
					newNode.proposer.ResponseCount++

					// choose V; value of highest-numbered proposal among those returned
					if base.IsNil(msg.V_a) && base.IsNil(newNode.proposer.V) {
						newNode.proposer.V = newNode.proposer.InitialValue
					} else if !base.IsNil(msg.V_a) && msg.N_a > newNode.proposer.N_a_max {
						newNode.proposer.N_a_max = msg.N_a
						newNode.proposer.V = msg.V_a
					}

					//responses := make([]base.Message, 0)
					for i := 0; i < len(newNode.peers); i++ {
						if msg.From() == newNode.peers[i] && newNode.proposer.Responses[i] == false {
							// only if peer hasn't responded yet
							newNode.proposer.Responses[i] = true
							newNode.proposer.SuccessCount++
						}
					}

					//newNode.SetResponse(responses)
					newNodes = append(newNodes, newNode)
				}
			} else {
				// just increment ResponseCount
				newNode := server.copy()
				newNode.proposer.Phase = Propose
				newNode.proposer.ResponseCount++
				newNodes = append(newNodes, newNode)
			}
		}

	case *AcceptResponse: // handle AcceptResponse
		msg := message.(*AcceptResponse)
		if msg.SessionId == server.proposer.SessionId {
			// Scenario 1: ACCEPT-OK received from majority; prepare DECIDE
			if msg.Ok {
				successCount := server.proposer.SuccessCount
				for i := 0; i < len(server.peers); i++ {
					// make sure message not from peer already seen
					if msg.From() == server.peers[i] && server.proposer.Responses[i] == false {
						successCount++ // increment success count
					}
				}
				// we can move onto Accept phase if we've reached majority
				if successCount >= (len(server.peers)/2)+1 {
					newNode := server.copy()
					newNode.proposer.Phase = Decide
					// set decided value
					newNode.agreedValue = newNode.proposer.V // setting this doesn't hurt unit_tests
					responses := make([]base.Message, 0)
					for _, peer := range newNode.peers {
						response := &DecideRequest{
							CoreMessage: base.MakeCoreMessage(newNode.Address(), peer),
							V:           newNode.proposer.V,
							SessionId:   msg.SessionId} // choose V; highest-numbered proposal among those returned
						responses = append(responses, response)
					}
					newNode.SetResponse(responses)
					// reset success and response count
					newNode.proposer.SuccessCount = 0
					newNode.proposer.ResponseCount = 0
					newNode.proposer.Responses = make([]bool, len(newNode.peers))
					newNodes = append(newNodes, newNode)
				}

				// Scenario 2: waiting for rest of the responses
				if server.proposer.ResponseCount+1 < len(server.peers) {
					newNode := server.copy()
					newNode.proposer.Phase = Accept
					newNode.proposer.ResponseCount++
					for i := 0; i < len(newNode.peers); i++ {
						if msg.From() == newNode.peers[i] && newNode.proposer.Responses[i] == false {
							// majority received, still waiting for others
							// increment successCount if peer did not already respond
							newNode.proposer.Responses[i] = true
							newNode.proposer.SuccessCount++
						}
					}
					newNodes = append(newNodes, newNode)
				}
			} else {
				// if ACCEPT reject - just increment response count
				newNode := server.copy()
				newNode.proposer.Phase = Accept
				newNode.proposer.ResponseCount++
				newNodes = append(newNodes, newNode)
			}

		}
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
	if base.IsNil(server.v_a) && base.IsNil(server.proposer.V) {
		server.proposer.V = server.proposer.InitialValue
	}
	server.proposer.SuccessCount = 0
	server.proposer.ResponseCount = 0
	server.proposer.Responses = make([]bool, len(server.peers))
	// then send ProposeRequest to all of it's peers

	responses := make([]base.Message, 0)
	from := server.Address()
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
