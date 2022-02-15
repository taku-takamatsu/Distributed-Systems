package pbservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"time"
	"viewservice"
)

//import "strconv"

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
	currView viewservice.View
	mu       sync.Mutex
}

func (pb *PBServer) Sync(args *SyncArgs, reply *SyncReply) error {
	// handle transfer of complete key/value database from primary to backup
	fmt.Println("Syncing..")
	for k, v := range args.Data {
		_, exist := pb.data[k]
		if !exist {
			pb.data[k] = v
		}
	}
	reply.Err = OK
	return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	// fmt.Println("PutArgs:", args)
	pb.data[args.Key] = args.Value
	// replicate to BACKUP
	if pb.me == pb.currView.Primary && pb.currView.Backup != "" {
		fmt.Println("Replicating..", pb.me)
		args := PutArgs{args.Key, args.Value, args.DoHash}
		var reply PutReply
		ok := call(pb.currView.Backup, "PBServer.Put", args, &reply)
		if !ok || reply.Err != OK {
			fmt.Println("Error replicating ")
		}
	}
	reply.Err = OK
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	fmt.Println("Server - GET received")
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.me != pb.currView.Primary {
		fmt.Println("GET ERROR: Wrong Server")
		reply.Err = ErrWrongServer
		return nil
	}
	if args.Key == "" {
		fmt.Println("GET ERROR: No key")
		reply.Err = ErrNoKey
		return nil
	}
	v, ok := pb.data[args.Key]
	if !ok {
		reply.Value = ""
	} else {
		reply.Value = v
	}
	reply.Err = OK
	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	pb.mu.Lock() // threaded function
	defer pb.mu.Unlock()
	view, _ := pb.vs.Ping(pb.currView.Viewnum)

	if view.Viewnum != pb.currView.Viewnum { // New View
		fmt.Println("New View", view.Viewnum, view.Primary, view.Backup)
		// if new backup, replicate primary key values
		if view.Backup != "" && view.Backup != pb.currView.Backup {
			args := SyncArgs{Data: pb.data}
			var reply SyncReply
			call(view.Backup, "PBServer.Sync", args, &reply)
			if reply.Err != OK {
				fmt.Println("Error replicating (TICK)")
			}
		}
		pb.currView = view
		fmt.Println("New CurrView:", pb.currView)
	}

	// if vx.Primary == pb.me {
	// 	fmt.Println("Primary:", pb.me)
	// } else if vx.Backup == pb.me {
	// 	pb.vs.Ping(pb.currView.Viewnum)
	// 	fmt.Println("Backup", pb.me)
	// } else {
	// 	fmt.Println("Neither", pb.me)
	// 	pb.vs.Ping(0)
	// }

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
	pb.data = make(map[string]string)
	pb.currView = viewservice.View{}

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
						fmt.Printf("shutdown: %v\n", err)
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
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
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
