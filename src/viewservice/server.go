package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"
import "runtime"
import "strings"


type ViewServer struct {
	mu                   sync.Mutex
	l                    net.Listener
	dead                 int32 // for testing
	rpccount             int32 // for testing
	me                   string

	// Your declarations here.
	currView             View                  //  keeps track of current view
	pingTimeMap          map[string]time.Time  //  keeps track of most recent time VS heard ping from each server
	primaryAckedCurrView bool                  //  keeps track of whether primary has ACKed the current view
	idleServer           string                //  keeps track of any idle servers
}


//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	vs.pingTimeMap[args.Me] = time.Now()
	if args.Me == vs.currView.Primary && args.Viewnum == vs.currView.Viewnum {
		vs.primaryAckedCurrView = true
	}
	if vs.currView.Viewnum == 0 {
		vs.currView.Primary = args.Me
		vs.currView.Viewnum = 1
		vs.primaryAckedCurrView = false
	} else if args.Me == vs.currView.Primary && args.Viewnum == 0 {
	} else if args.Me != vs.currView.Primary && vs.currView.Backup == "" && vs.primaryAckedCurrView {
		vs.idleServer = args.Me
		vs. currView.Viewnum++
		vs.primaryAckedCurrView = false
	} else if args.Me != vs.currView.Primary && args.Me != vs.currView.Backup {
		vs.idleServer = args.Me
	}

	reply.View = vs.currView
	return nil
}


//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Your code here.	

	// Add view to the reply message
	reply.View = vs.currView

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

	// Your code here.	

	// 1. No recent pings from the idle server
	isDead := func(s string) bool {
		if s == "" { return true }
		t, ok := vs.pingTimeMap[s]
		return !ok || time.Since(t) > PingInterval*DeadPings
	}
	if isDead(vs.idleServer) {
		vs.idleServer = ""
	}

	// 2. No recent pings from the backup
	if vs.primaryAckedCurrView {
		
		if isDead(vs.currView.Primary) {
			vs.currView.Primary = vs.currView.Backup
			vs.currView.Backup = ""
			
			if !isDead(vs.idleServer) {
				vs.currView.Backup = vs.idleServer
				vs.idleServer = ""
			}
			vs.currView.Viewnum++
			vs.primaryAckedCurrView = false
		} else if isDead(vs.currView.Backup) || vs.currView.Backup == "" {
			if vs.idleServer != "" && !isDead(vs.idleServer) {
				vs.currView.Backup = vs.idleServer
				vs.idleServer = ""
				vs.currView.Viewnum++
				vs.primaryAckedCurrView = false
			}
		}
	}

}

func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}


// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}


func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me


	if runtime.GOOS == "windows" {
		vs.me = strings.Replace(vs.me, "/var/tmp", ".", 1)
		vs.me = strings.Replace(vs.me, "/", "_", -1) 
		// 예: /var/tmp/824-1/viewserver-x -> ._var_tmp_824-1_viewserver-x
    }
	// Your vs.* initializations here.
	vs.currView = View{Viewnum: 0, Primary: "", Backup: ""}
	vs.pingTimeMap = make(map[string]time.Time)
	vs.primaryAckedCurrView = false
	vs.idleServer = ""

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
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
