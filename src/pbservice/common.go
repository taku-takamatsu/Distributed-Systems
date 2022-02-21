package pbservice

import "hash/fnv"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.
	Id int64
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id int64
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
//RPC's to forward client requests from primary to backup

type PutBackupArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.
	Id int64
	// Field names must start with capital letters,
	// otherwise RPC will break.
}
type PutBackupReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetBackupArgs struct {
	Key string
	// You'll have to add definitions here.
	Id int64
}

type GetBackupReply struct {
	Err   Err
	Value string
}

// keep track of states and its error status
type PutState struct {
	Value string
	Err   Err
}

type GetState struct {
	Value string
	Err   Err
}
type SyncArgs struct {
	Data     map[string]string
	PutState map[int64]*PutState
	GetState map[int64]*GetState
}

type SyncReply struct {
	Err Err
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
