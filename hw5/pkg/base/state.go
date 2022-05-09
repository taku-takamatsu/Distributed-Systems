package base

import (
	"encoding/binary"
	"hash/fnv"
	"log"
	"math/rand"
)

type State struct {
	nodes      map[Address]Node
	addresses  []Address
	blockLists map[Address][]Address
	Network    []Message
	Depth      int

	isDropOff   bool
	isDuplicate bool

	// inheritance
	Prev  *State
	Event Event

	// auxiliary information
	nodeHash    uint64
	networkHash uint64
	// If the network is sorted by hash, then no need to do it again
	// Thus, use this indicator to record if it has been sorted
	hashSorted bool
}

func NewState(depth int, isDropOff, isDuplicate bool) *State {
	return &State{
		nodes:       map[Address]Node{},
		blockLists:  map[Address][]Address{},
		Network:     make([]Message, 0, 8),
		Depth:       depth,
		isDropOff:   isDropOff,
		isDuplicate: isDuplicate,
		nodeHash:    0,
		networkHash: 0,
		hashSorted:  false,
	}
}

func (s *State) AddNode(address Address, node Node, blockList []Address) {
	// seems to be handling duplicates by replacing it?
	if old, ok := s.nodes[address]; ok {
		s.nodeHash -= old.Hash()
	} else {
		s.addresses = append(s.addresses, address)
	}

	s.nodes[address] = node
	s.blockLists[address] = blockList
	s.nodeHash += node.Hash()
	return
}

func (s *State) UpdateNode(address Address, node Node) {
	if old, ok := s.nodes[address]; ok {
		s.nodeHash -= old.Hash()
	} else {
		log.Printf("node does not exist")
		panic("node does not exist")
	}

	s.nodes[address] = node
	s.nodeHash += node.Hash()
	s.Receive(node.HandlerResponse())
}

func (s *State) Nodes() map[Address]Node {
	return s.nodes
}

func (s *State) GetNode(address Address) Node {
	return s.nodes[address]
}

func (s *State) Send(meg Message) {
	s.Network = append(s.Network, meg)
	return
}

func (s *State) Clone() *State {
	newState := NewState(s.Depth+1, s.isDropOff, s.isDuplicate)
	for address, node := range s.nodes {
		newState.nodes[address] = node
	}

	// Assume these fields are identical among every state and their children
	newState.addresses = s.addresses
	newState.blockLists = s.blockLists

	for _, message := range s.Network {
		newState.Network = append(newState.Network, message)
	}

	newState.nodeHash = s.nodeHash
	newState.networkHash = s.networkHash
	newState.hashSorted = s.hashSorted

	return newState
}

func (s *State) Inherit(event Event) *State {
	newState := s.Clone()
	newState.Prev = s
	newState.Event = event
	return newState
}

// blockList will not be compared in the Equal operation
func (s *State) Equals(other *State) bool {
	if other == nil {
		return false
	}

	if len(s.nodes) != len(other.nodes) || len(s.Network) != len(other.Network) ||
		s.nodeHash != other.nodeHash || s.networkHash != other.networkHash {
		return false
	}

	for address, node := range s.nodes {
		otherNode, ok := other.nodes[address]
		if !ok || !node.Equals(otherNode) {
			return false
		}
	}

	if !s.hashSorted {
		hashSort(s.Network)
		s.hashSorted = true
	}

	if !other.hashSorted {
		hashSort(other.Network)
		other.hashSorted = true
	}

	for i, message := range s.Network {
		if !message.Equals(other.Network[i]) {
			return false
		}
	}

	return true
}

func isBlocked(blockList []Address, candidate Address) bool {
	if blockList == nil {
		return false
	}

	for _, addr := range blockList {
		if addr == candidate {
			return true
		}
	}

	return false
}

func (s *State) isLocalCall(index int) bool {
	message := s.Network[index]
	return message.From() == message.To()
}

func (s *State) isMessageReachable(index int) (bool, *State) {
	message := s.Network[index]
	to := message.To()

	_, ok := s.nodes[to]
	if !ok {
		newState := s.Inherit(UnknownDestinationEvent(message))
		newState.DeleteMessage(index)
		return false, newState
	}

	if isBlocked(s.blockLists[to], message.From()) {
		newState := s.Inherit(PartitionEvent(message))
		newState.DeleteMessage(index)
		return false, newState
	}

	return true, nil
}

// Three cases: 1) local call 2) message arrives normally, 3) message arrives duplicated
// Returns: array of states
// handle message and send back a response message into the network
// handling a message may result in more than one state
func (s *State) HandleMessage(index int, deleteMessage bool) (result []*State) {
	message := s.Network[index]       // get message
	to := message.To()                // node this message is sending to
	var newStates = make([]*State, 0) // list of new states
	// collect all possible nodes that may be created from this message
	oldNode := s.nodes[to]
	// derive new nodes form old node
	newNodes := oldNode.MessageHandler(message) // cap:3
	for _, newNode := range newNodes {          // handle each new node
		newState := s.Inherit(HandleEvent(message)) // generate new state
		if deleteMessage {                          // if delete == true
			newState.DeleteMessage(index)
		}
		newState.UpdateNode(to, newNode)
		newStates = append(newStates, newState)
	}

	//log.Printf("HandleMessage: newStates=%v", newStates)
	return newStates
}

func (s *State) DeleteMessage(index int) {
	// Remove the i-th message
	message := s.Network[index]
	s.Network[index] = s.Network[len(s.Network)-1]
	s.Network = s.Network[:len(s.Network)-1]

	// remove from the hash
	s.networkHash -= message.Hash()
	s.hashSorted = false
}

func (s *State) Receive(messages []Message) {
	for _, message := range messages {
		s.Network = append(s.Network, message)
		s.networkHash += message.Hash()
		s.hashSorted = false
	}
}

func (s *State) NextStates() []*State {
	nextStates := make([]*State, 0, 4)

	for i := range s.Network {
		// i == Message[i]

		// check if it is a local call
		if s.isLocalCall(i) { // isLocalCall checks from == true
			newStates := s.HandleMessage(i, true)
			nextStates = append(nextStates, newStates...)
			continue
		}

		// check Network Partition
		reachable, newState := s.isMessageReachable(i)
		if !reachable {
			nextStates = append(nextStates, newState)
			continue
		}

		// If case (a) or (b) does not occur (ie. no call + partition)
		// then the arrival of a message should create at least 3 new states
		// TODO: Drop off a message
		// A message is dropped during transmission
		if s.isDropOff {
			//log.Printf("NextStates: drop off")
			message := s.Network[i]
			newStateDO := s.Inherit(DropOffEvent(message))
			newStateDO.DeleteMessage(i)                 // delete message
			nextStates = append(nextStates, newStateDO) // append new State
		}

		// TODO: Message arrives Normally. (use HandleMessage)

		//log.Printf("NextStates: message arrives normally")
		newStates := s.HandleMessage(i, true)
		nextStates = append(nextStates, newStates...) // append new State

		// TODO: Message arrives but the message is duplicated. The same message may come later again
		// (use HandleMessage)
		if s.isDuplicate {
			//log.Printf("NextStates: duplicate")
			newStatesDup := s.HandleMessage(i, false)        // don't delete message
			nextStates = append(nextStates, newStatesDup...) // append new State
		}

	}

	// You must iterate through the addresses, because every iteration on map is random...
	// Weird feature in Go
	for _, address := range s.addresses { // iterate every node and trigger their next timer
		node := s.nodes[address]
		//TODO: call the timer (use TriggerNodeTimer)
		nextStates = append(nextStates, s.TriggerNodeTimer(address, node)...)
	}

	return nextStates
}

func (s *State) TriggerNodeTimer(address Address, node Node) []*State {
	// TODO: implement it
	// every timer should lead to a new state;
	// (if N nodes with timers, generate n new states in parallel)
	newStates := make([]*State, 0)
	newNodes := node.TriggerTimer() // Trigger the next timer and return a list of new nodes.
	for _, newNode := range newNodes {
		newState := s.Inherit(TriggerEvent(address, node.NextTimer()))
		newState.UpdateNode(address, newNode)
		newStates = append(newStates, newState)
	}
	return newStates
}

func (s *State) RandomNextState() *State {
	// Randomly returns 1 single state out of all possible next-states
	// Still need to consider all possible transitions

	timerAddresses := make([]Address, 0, len(s.nodes))
	for addr, node := range s.nodes {
		if IsNil(node.NextTimer()) {
			continue
		}
		timerAddresses = append(timerAddresses, addr)
	}
	// handle empty event
	// Ref: https://edstem.org/us/courses/19078/discussion/1459780
	if len(s.Network) == 0 && len(timerAddresses) == 0 {
		// empty event
		return s.Inherit(EmptyEvent())
	}
	roll := rand.Intn(len(s.Network) + len(timerAddresses))

	if roll < len(s.Network) {
		// check Network Partition
		reachable, newState := s.isMessageReachable(roll) // roll == index
		if !reachable {
			return newState
		}

		// TODO: handle message and return one state
		// still need to consider all possible states;
		nextStates := s.HandleMessage(roll, true)
		return nextStates[0] // return that roll
	}

	// TODO: trigger timer and return one state
	address := timerAddresses[roll-len(s.Network)]
	node := s.nodes[address]
	states := s.TriggerNodeTimer(address, node)
	return states[0]
}

// Calculate the hash function of a State based on its nodeHash and networkHash.
// It doesn't consider the group information because we assume the group information does not change
// during the evaluation.
func (s *State) Hash() uint64 {
	b := make([]byte, 8)
	h := fnv.New64()

	binary.BigEndian.PutUint64(b, s.nodeHash)
	_, _ = h.Write(b)

	binary.BigEndian.PutUint64(b, s.networkHash)
	_, _ = h.Write(b)

	return h.Sum64()
}
