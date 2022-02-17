package pbservice

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net/rpc"
	"time"
	"viewservice"
)

// You'll probably need to uncomment these:

// Function to generate numbers with high probability of being unique
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	primary string
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ck.primary = ck.vs.Primary() // client should never contact VS unless needed
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
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {
	//get primary if not set
	if ck.primary == "" {
		ck.primary = ck.vs.Primary()
	}
	args := &GetArgs{key, nrand()} // Key and Id
	reply := GetReply{}
	ok := call(ck.primary, "PBServer.Get", args, &reply)
	// keep trying if we get an error
	for (reply.Err != OK || !ok) && ck.primary != "" {
		fmt.Println("Error calling GET", reply)
		// try again
		time.Sleep(viewservice.PingInterval)
		reply = GetReply{}
		ck.primary = ck.vs.Primary() // reassign from VS
		ok = call(ck.primary, "PBServer.Get", args, &reply)
	}
	fmt.Println("GET success:", key, reply.Value)
	return reply.Value
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	if ck.primary == "" {
		fmt.Println("Getting primary:", ck.primary)
		ck.primary = ck.vs.Primary()
	}
	args := &PutArgs{key, value, dohash, nrand()}
	reply := PutReply{}
	ok := call(ck.primary, "PBServer.Put", args, &reply)
	for (reply.Err != OK || !ok) && ck.primary != "" {
		// try again after Ping Interval
		fmt.Println("Error calling PUT", reply, "retrying...")
		time.Sleep(viewservice.PingInterval)
		reply = PutReply{}
		ck.primary = ck.vs.Primary() // re-assign on error
		ok = call(ck.primary, "PBServer.Put", args, &reply)
	}
	fmt.Println("PUT success: k:", key, "v:", value)
	return reply.PreviousValue // only used by PutHash
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}
