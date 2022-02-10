package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	servers      map[string]time.Time
	currentView  View
	primaryAcked bool
	idle         chan string //keep track of idle servers
}

// helper as we update the view many times...
func (vs *ViewServer) NewView(p string, b string) {
	// doesn't need lock as calling functions all use mutex
	vs.currentView.Viewnum++
	vs.currentView.Primary = p
	vs.currentView.Backup = b
	vs.primaryAcked = false
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	address := args.Me
	viewNum := args.Viewnum

	fmt.Println("Ping:", address, viewNum)
	vs.servers[address] = time.Now() //update time

	// Ping(0): new/restarted server
	if viewNum == 0 {
		if vs.currentView.Primary == "" { // First primary
			fmt.Println("Setting primary")
			vs.NewView(address, "")
		} else if vs.currentView.Backup == "" { // First backup
			fmt.Println("Setting backup")
			vs.NewView(vs.currentView.Primary, address)
		} else if vs.currentView.Primary == address { //Primary crashed, restarted
			fmt.Println("Primary crashed and restarted")
			//primary crashed, promote backup and set old primary as new backup
			// Reference: https://edstem.org/us/courses/19078/discussion/1101378
			if vs.primaryAcked { // only update View if Acked
				vs.NewView(vs.currentView.Backup, address)
			}
			fmt.Println("Promoted Backup:", vs.currentView.Primary, vs.currentView.Backup)
		} else if vs.currentView.Primary != address && vs.currentView.Backup != address {
			// primary and backup filled, insert new server to idle channel
			fmt.Println("Idle server")
			go func() { vs.idle <- address }()
		}
	} else if viewNum == vs.currentView.Viewnum && vs.currentView.Primary == address {
		//acked
		fmt.Println(viewNum, "ACKED")
		vs.primaryAcked = true
	}

	reply.View = vs.currentView
	fmt.Println("REPLY:", reply.View.Primary, reply.View.Backup, reply.View.Viewnum)
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	// Responds with current view
	vs.mu.Lock()
	defer vs.mu.Unlock()
	reply.View = vs.currentView
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	latestPrimaryTime := vs.servers[vs.currentView.Primary]
	if vs.primaryAcked && latestPrimaryTime.Before(time.Now().Add(time.Duration(-PingInterval)*DeadPings)) {
		fmt.Println("PRIMARY DEAD", vs.currentView.Primary, vs.currentView.Viewnum)
		//promote backup to primary - akin to New View
		vs.NewView(vs.currentView.Backup, "")
		vs.servers[vs.currentView.Primary] = time.Now() // update time

		select {
		case idle := <-vs.idle:
			if idle != vs.currentView.Backup && idle != vs.currentView.Primary {
				fmt.Println("Pulling from idle server")
				vs.currentView.Backup = idle
				vs.servers[vs.currentView.Backup] = time.Now()
			}
		default:
			delete(vs.servers, vs.currentView.Backup) // delete time
			vs.currentView.Backup = ""

			fmt.Println("Promoting Backup to Primary", vs.currentView.Viewnum, vs.currentView.Primary, vs.currentView.Backup)
		}
	}
	latestBackupTime, exist := vs.servers[vs.currentView.Backup]
	// Promoting backup is akin to setting up new View -- we can't do this until primary is acked
	if exist && vs.primaryAcked && latestBackupTime.Before(time.Now().Add(time.Duration(-PingInterval)*DeadPings)) {
		fmt.Println("BACKUP DEAD", vs.currentView.Backup, vs.currentView.Viewnum)
		select {
		case idle := <-vs.idle:
			if idle != vs.currentView.Backup && idle != vs.currentView.Primary {
				fmt.Println("Pulling from idle server")
				vs.currentView.Backup = idle
				vs.servers[vs.currentView.Backup] = time.Now()
			}
		default:
			delete(vs.servers, vs.currentView.Backup) // delete time
			vs.currentView.Backup = ""

			fmt.Println("Promoting Idle to Backup", vs.currentView.Viewnum, vs.currentView.Primary, vs.currentView.Backup)
		}
		vs.primaryAcked = false
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currentView = View{0, "", ""}
	vs.servers = make(map[string]time.Time)
	vs.primaryAcked = false
	vs.idle = make(chan string, 5) // buffered

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
