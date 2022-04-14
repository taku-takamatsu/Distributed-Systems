package shardkv

import (
	"crypto/rand"
	"hash/fnv"
	"math/big"
	"shardmaster"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	TimeOut       = "TimeOut"
	PUT           = "PUT"
	PUTHASH       = "PUTHASH"
	GET           = "GET"
	// The recommended approach is to have each replica group
	// use Paxos to log not only the sequence of Puts and Gets
	// but also the sequence of reconfigurations.
	RECONFIG = "RECONFIG"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	TxId     int64
	ClientId int64
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	TxId     int64
	ClientId int64
}

type GetReply struct {
	Err   Err
	Value string
}

type ReConfigArgs struct {
	TxId     int64
	SeenOps  map[int]map[int64]bool    // indexed by TxId
	Previous map[int]map[int64]string  // indexed by ClientId
	Data     map[int]map[string]string //kv
	Config   shardmaster.Config
}

type ReConfigReply struct {
	Err string
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
