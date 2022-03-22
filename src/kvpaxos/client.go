package kvpaxos

import (
	"fmt"
	"net/rpc"
	"sync"
	"time"
)

type Clerk struct {
	servers []string
	// You will have to modify this struct.
	replica int
	me      int64
	mu      sync.Mutex // to ensure that client has only one outstanding get or put
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.replica = 0 // index of the replica
	ck.me = nrand()
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// Try each server consecutively
//
func (ck *Clerk) GetServer() string {
	s := ck.servers[ck.replica]
	ck.replica++
	if ck.replica >= len(ck.servers) {
		ck.replica = 0
	}
	return s
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs{key, nrand(), ck.me}
	var reply GetReply
	// log.Printf("Client: GET k=%v, id=%v", args.Key, args.Id)
	ok := call(ck.GetServer(), "KVPaxos.Get", args, &reply)
	for !ok || (reply.Err != OK && reply.Err != ErrNoKey) {
		// log.Printf("Client: GET; retrying... id=%v", args.Id)
		time.Sleep(100 * time.Millisecond)
		// var reply GetReply
		ok = call(ck.GetServer(), "KVPaxos.Get", args, &reply)
	}
	// log.Printf("Client: GET done id=%v, k=%v", args.Id, args.Key)
	return reply.Value
}

//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutArgs{key, value, dohash, nrand(), ck.me}
	var reply PutReply
	// log.Printf("Client: PUT k=%v dohash=%v, id=%v", args.Key, args.DoHash, args.Id)
	ok := call(ck.GetServer(), "KVPaxos.Put", args, &reply)
	for !ok || reply.Err != OK {
		// log.Printf("Client: PUT; ok=%v, reply=%v; retrying... id=%v", ok, reply, args.Id)
		time.Sleep(100 * time.Millisecond)
		// var reply PutReply
		ok = call(ck.GetServer(), "KVPaxos.Put", args, &reply)
	}
	// log.Printf("Client: PUT done k=%v, prevVal=%v, id=%v", args.Key, args.Value, args.Id)
	return reply.PreviousValue
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}
