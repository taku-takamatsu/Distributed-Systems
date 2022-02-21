package pbservice

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
	"viewservice"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	// Your declarations here.
	data     map[string]string
	putState map[int64]*PutState // putState[xid] to remember duplicate PUT calls
	getState map[int64]*GetState // getState[xid] to remember duplicate GET calls
	currView viewservice.View
	mu       sync.Mutex
}

func (pb *PBServer) Sync(args *SyncArgs, reply *SyncReply) error {
	// handle transfer of complete key/value database from primary to backup
	// make sure p/b are in sync
	log.Println("Server: Syncing..")
	for k, v := range args.Data {
		pb.data[k] = v
	}
	for k, v := range args.PutState {
		pb.putState[k] = &PutState{v.Value, v.Err}
	}
	for k, v := range args.GetState {
		pb.getState[k] = &GetState{v.Value, v.Err}
	}
	reply.Err = OK
	log.Println("Server: Sync Complete;", reply.Err)
	return nil
}

func (pb *PBServer) PutBackup(args *PutBackupArgs, reply *PutBackupReply) error {
	//handle client PUT requests from primary to backup
	// maintain state in backup as well
	pb.mu.Lock()
	log.Println("SERVER: (Backup) PUT received", args)

	if pb.currView.Backup != pb.me {
		reply.Err = ErrWrongServer
		pb.mu.Unlock()
		return errors.New("SERVER: (Backup) PUT ErrWrongServer")
	}

	id := args.Id
	key := args.Key
	value := args.Value

	prevVal := pb.data[key] // if not exist, evalutes to "" (zero value of string type)
	pb.data[key] = value    // set value
	reply.Err = OK
	pb.putState[id] = &PutState{prevVal, reply.Err}
	log.Println("SERVER: (Backup) Finished PUT - addr;", pb.me, "id;", id, "key;", key, "value;", pb.data[key])
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	pb.mu.Lock()
	log.Println("SERVER: (Primary) PUT received", args, "p:", pb.me)

	if pb.currView.Primary != pb.me { // wrong server
		reply.Err = ErrWrongServer
		pb.mu.Unlock() //releasing locks early on error ref: Ed
		return errors.New("SERVER: (Primary) PUT ErrWrongServer")
	}

	id := args.Id
	key := args.Key
	value := args.Value

	//handle duplicates; at-most-once semantic
	prevState, e := pb.putState[id]
	if e {
		log.Println("SERVER: (Primary) Duplicate PUT request - ID:", args.Id, "previous value:", prevState)
		reply.PreviousValue = prevState.Value
		reply.Err = prevState.Err
		pb.mu.Unlock()
		return nil
	}

	//putHash()
	prevVal := pb.data[key] // if not exist, evalutes to "" (zero value of type)
	if args.DoHash {
		value = strconv.Itoa(int(hash(prevVal + value)))
	}

	// try replicate to backup
	if pb.currView.Backup != "" {
		backupArgs := &PutBackupArgs{key, value, args.DoHash, id}
		backupReply := PutBackupReply{}

		// log.Println("SERVER: Replicating to backup;", pb.currView.Backup, "id:", id)
		ok := call(pb.currView.Backup, "PBServer.PutBackup", backupArgs, &backupReply)
		if !ok || backupReply.Err != OK { // error replicating, then return false to retry
			reply.Err = backupReply.Err
			pb.mu.Unlock()
			return errors.New("SERVER: (Primary) Error replicating to backup")
		}
	}

	pb.data[key] = value // set value
	reply.PreviousValue = prevVal
	reply.Err = OK
	pb.putState[id] = &PutState{prevVal, reply.Err}
	log.Println("SERVER: (Primary) Finished PUT - addr;", pb.me, "id;", id, "key;", key, "value;", pb.data[key])
	pb.mu.Unlock()

	return nil
}

func (pb *PBServer) GetBackup(args *GetBackupArgs, reply *GetBackupReply) error {
	pb.mu.Lock()
	// log.Println("SERVER: (Backup) GET received - key:", args.Key, "with id:", args.Id)
	if pb.currView.Backup != pb.me {
		reply.Err = ErrWrongServer
		pb.mu.Unlock()
		return errors.New("SERVER: (Backup) GET ErrWrongServer")
	}

	key := args.Key
	id := args.Id

	// update k/v store
	v, exist := pb.data[key]
	if !exist {
		reply.Value = ""
		reply.Err = ErrNoKey
	} else {
		reply.Value = v
		reply.Err = OK
	}

	// store state
	pb.getState[id] = &GetState{reply.Value, reply.Err}
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	log.Println("SERVER: (Primary) GET received - key:", args.Key, "with id:", args.Id)

	if pb.currView.Primary != pb.me {
		reply.Err = ErrWrongServer
		pb.mu.Unlock()
		return errors.New("SERVER: (Primary) GET ErrWrongServer")
	}

	key := args.Key
	id := args.Id

	// at most once semantic; check for duplicates
	prevState, e := pb.getState[id]
	if e {
		// log.Println("SERVER: (Primary) Duplicate GET request - ID;", args.Id, "key;", key, "value:", pb.getState[id])
		reply.Value = prevState.Value
		reply.Err = prevState.Err
		pb.mu.Unlock()
		return nil
	}

	v, exist := pb.data[key]

	// send request to BACKUP
	if pb.currView.Backup != "" {
		// log.Println("SERVER: Retrieveing GET from backup - key:", key)
		backupArgs := GetBackupArgs{key, id}
		backupReply := GetBackupReply{}
		ok := call(pb.currView.Backup, "PBServer.GetBackup", backupArgs, &backupReply)
		if !ok || backupReply.Value != v { // backup may have switched to primary or died
			reply.Err = backupReply.Err
			pb.mu.Unlock()
			return errors.New("SERVER: (Primary) GET Backup Failure")
		}
	}

	if !exist {
		reply.Value = ""
		reply.Err = ErrNoKey
	} else {
		reply.Value = v
		reply.Err = OK
	}
	pb.getState[id] = &GetState{reply.Value, reply.Err}
	log.Println("SERVER: (Primary) Finished GET - addr;", pb.me, "id;", id, "key;", key, "value;", reply.Value)
	pb.mu.Unlock()

	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	pb.mu.Lock() // threaded function
	defer pb.mu.Unlock()
	view, _ := pb.vs.Ping(pb.currView.Viewnum)
	// log.Println("SERVER: Ping()", view)
	if view.Viewnum != pb.currView.Viewnum { // New View
		// log.Println("SERVER: New View", view.Viewnum, view.Primary, view.Backup)
		// as long as backup exist and primary is up-to-date, sync
		if view.Primary == pb.me && view.Backup != "" {
			args := &SyncArgs{Data: pb.data, PutState: pb.putState, GetState: pb.getState}
			var reply SyncReply
			ok := call(view.Backup, "PBServer.Sync", args, &reply)
			if reply.Err != OK || !ok {
				log.Println("SERVER: Error syncing new view to backup", reply)
				return // don't update currView
			}
		}
		pb.currView = view
		// log.Println("SERVER: New CurrView:", pb.me, pb.currView)
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.currView = viewservice.View{}
	pb.data = make(map[string]string)
	pb.putState = make(map[int64]*PutState)
	pb.getState = make(map[int64]*GetState)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)
	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						log.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				log.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
