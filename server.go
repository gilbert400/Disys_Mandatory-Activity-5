package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	pb "github.com/gilbert400/Disys_Mandatory-Activity-5/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const basePort = 8000

type AuctionServer struct {
	pb.UnimplementedAuctionServerServer

	id    int32
	addr  string
	peers map[int32]string

	isLeader bool
	leaderId int32

	seqNo      int64
	appliedSeq int64

	mu            sync.Mutex
	highestBid    int
	highestBidder string
	bidders       map[string]bool
	lastBid       map[string]int
	seenReq       map[string]bool

	auctionOver   bool
	lastHeartbeat time.Time
}

// Method that receives an announcement of a new leader
func (s *AuctionServer) AnnounceNewLeader(ctx context.Context, in *pb.NodeInfo) (*pb.Reply, error) {
	s.mu.Lock()
	s.leaderId = in.Id
	s.isLeader = (s.leaderId == s.id)

	//Records the current time of the announcement of leader as if it was a heartbeat, to not
	//start a new election.
	if !s.isLeader {
		s.lastHeartbeat = time.Now()
	}
	s.mu.Unlock()
	log.Printf("node %d: new leader announced: %d", s.id, s.leaderId)
	return &pb.Reply{}, nil
}

// Heartbeat function. Records the time of this heartbeat.
func (s *AuctionServer) HeartBeat(ctx context.Context, req *pb.Reply) (*pb.Reply, error) {
	s.mu.Lock()
	if !s.isLeader {
		s.lastHeartbeat = time.Now()
	}
	s.mu.Unlock()
	return &pb.Reply{}, nil
}

// When an election is started. It returns its own information for the node
// that received noticed the primary node was down, as it is that node that decides who will become the leader.
func (s *AuctionServer) StartElection(ctx context.Context, in *pb.Reply) (*pb.NodeInfo, error) {
	log.Printf("node %d: received StartElection", s.id)
	return &pb.NodeInfo{Id: s.id, Addr: s.addr}, nil
}

// Returns the result of the election.
func (s *AuctionServer) Result(context.Context, *pb.Reply) (*pb.Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.auctionOver {
		return &pb.Outcome{
			Result:        "Auction ended",
			HighestBid:    int32(s.highestBid),
			HighestBidder: s.highestBidder,
		}, nil
	}

	return &pb.Outcome{
		Result:        "Auction ongoing",
		HighestBid:    int32(s.highestBid),
		HighestBidder: s.highestBidder,
	}, nil
}

// Bid function
func (s *AuctionServer) Bid(ctx context.Context, req *pb.BidEntry) (*pb.Reply, error) {
	s.mu.Lock()

	//If this node is not the leader, it forwards the information to the leader.
	if !s.isLeader {
		s.mu.Unlock()
		log.Printf("node %d: forwarding bid to leader %d", s.id, s.leaderId)
		return s.forwardBidToLeader(ctx, req)
	}

	defer s.mu.Unlock()

	//Rejects bid if auction is over
	if s.auctionOver {
		log.Printf("node %d: bid rejected, auction over", s.id)
		return &pb.Reply{}, nil
	}

	//Gets the bidder and amount information
	bidder := req.GetBidder()
	amount := int(req.GetAmount())

	//Registeres new bodder
	if _, ok := s.bidders[bidder]; !ok {
		s.bidders[bidder] = true
		log.Printf("node %d: registered new bidder %s", s.id, bidder)
	}

	//Checks if the current bid from the same client is lower than the last bid from the same client.
	if last, ok := s.lastBid[bidder]; ok && amount <= last {
		log.Printf("node %d: bid from %s rejected, not higher than previous", s.id, bidder)
		return &pb.Reply{}, nil
	}

	//Checks if the bid is lower than the current highest from any bidder. If it is it rejects it.
	if amount <= s.highestBid {
		log.Printf("node %d: bid from %s rejected, not higher than highest", s.id, bidder)
		return &pb.Reply{}, nil
	}

	log.Printf("node %d: replicating bid %s:%d to peers", s.id, bidder, amount)

	var wg sync.WaitGroup

	//We send the information of bidders to all the replicated nodes
	for _, peerAddr := range s.peers {
		//We have a waitgroup, so all updates to all nodes can happen in parallel, but we wait for all
		//Calls to be done.
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			//We choose to have a timeout if a node has crashed.
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			conn, err := grpc.DialContext(ctx, addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("node %d: failed to connect to %s for replication: %v", s.id, addr, err)
				return
			}
			defer conn.Close()

			//We make a new client and replicate the bid to the node.
			client := pb.NewAuctionServerClient(conn)
			_, err = client.ReplicateBid(ctx, req)
			if err != nil {
				log.Printf("node %d: failed to replicate to %s: %v", s.id, addr, err)
				return
			}
			log.Printf("node %d: successfully replicated to %s", s.id, addr)
		}(peerAddr)
	}
	wg.Wait()

	//We update the current bidding information.
	s.lastBid[bidder] = amount
	s.highestBid = amount
	s.highestBidder = bidder

	log.Printf("node %d: successfully committed bid - %s: %d", s.id, bidder, amount)
	return &pb.Reply{}, nil
}

// This method replicates the bid in replicated nodes
func (s *AuctionServer) ReplicateBid(ctx context.Context, entry *pb.BidEntry) (*pb.Reply, error) {
	log.Printf("node %d: applying replicated bid from leader - %s:%d", s.id, entry.GetBidder(), entry.GetAmount())

	s.mu.Lock()
	defer s.mu.Unlock()

	//We get the bidder and amount info
	bidder := entry.GetBidder()
	amount := int(entry.GetAmount())

	//We register any new bidders
	if _, ok := s.bidders[bidder]; !ok {
		s.bidders[bidder] = true
		log.Printf("node %d: registered new bidder %s from replication", s.id, bidder)
	}

	//We update the last bid of this bidder
	s.lastBid[bidder] = amount

	//We check whether the current amount is higher than the last recorded highest bid. If Yes we update.
	if amount > s.highestBid {
		s.highestBid = amount
		s.highestBidder = bidder
	}

	log.Printf("node %d: successfully applied replicated bid - %s: %d", s.id, bidder, amount)
	return &pb.Reply{}, nil
}

// This forwards information to the leader if info has been received by client from a backup node.
func (s *AuctionServer) forwardBidToLeader(ctx context.Context, req *pb.BidEntry) (*pb.Reply, error) {
	//We get the leader information
	leaderId := s.leaderId
	port := 8000 + int(leaderId)
	addr := "localhost:" + strconv.Itoa(port)
	log.Printf("node %d: dialing leader at %s", s.id, addr)

	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Printf("node %d: failed to reach leader %d at %s: %v", s.id, leaderId, addr, err)
		return nil, err
	}
	defer conn.Close()

	//We make a client and send the request to the leader.
	client := pb.NewAuctionServerClient(conn)
	return client.Bid(ctx, req)
}

// Here we run an election when a leader is down.
func (s *AuctionServer) runElection() {
	s.mu.Lock()

	//If this should be the current leader we exit.
	if s.isLeader {
		s.mu.Unlock()
		return
	}

	//We replicate the info to not block other processes from accessing.
	leaderId := s.leaderId
	idLocal := s.id
	peersCopy := make(map[int32]string)

	//We create a copy of peers for same reason as above.
	for k, v := range s.peers {
		peersCopy[k] = v
	}
	s.mu.Unlock()

	log.Printf("node %d: starting election, current leaderId=%d", idLocal, leaderId)

	isLeader := false

	//We loop as long as no leader with a successfull connection has been established.
	for {

		//If the next incremental id is equal to this node, we set this node as the leader.
		if leaderId+1 == idLocal {
			isLeader = true
			leaderId = idLocal
			log.Printf("node %d: no higher candidate, becoming leader", idLocal)
			break
		}

		nextId := leaderId + 1
		addrNext, ok := peersCopy[nextId]

		//if the entry does not exists, we continue
		if !ok {
			leaderId = nextId
			continue
		}

		//We check if there is a valid connection. If not retry with the next peer.
		log.Printf("node %d: contacting node %d at %s for election", idLocal, nextId, addrNext)
		conn, err := grpc.Dial(addrNext, grpc.WithInsecure())
		if err != nil {
			log.Printf("node %d: failed to dial node %d: %v", idLocal, nextId, err)
			leaderId = nextId
			continue
		}

		//Try to call start election. if no response try with the next it.
		client := pb.NewAuctionServerClient(conn)
		nodeInfo, err := client.StartElection(context.Background(), &pb.Reply{})
		conn.Close()
		if err != nil {
			log.Printf("node %d: node %d did not respond to StartElection: %v", idLocal, nextId, err)
			leaderId = nextId
			continue
		}

		//If responded, we send the new leader to all nodes.
		log.Printf("node %d: node %d responded to election, announcing leader %d", idLocal, nodeInfo.Id, nodeInfo.Id)
		for peerId, peerAddr := range peersCopy {
			if peerId == idLocal {
				continue
			}
			nodeInfo := nodeInfo
			go func(addr string) {
				peerConn, peerErr := grpc.Dial(addr, grpc.WithInsecure())
				if peerErr != nil {
					return
				}
				defer peerConn.Close()

				peerClient := pb.NewAuctionServerClient(peerConn)
				peerClient.AnnounceNewLeader(context.Background(), nodeInfo)
			}(peerAddr)
		}

		//We update this node aswell
		isLeader = nodeInfo.Id == idLocal
		leaderId = nodeInfo.Id
		break
	}
	//We set the global variables to the valyues of the local ones.
	s.mu.Lock()
	s.isLeader = isLeader
	s.leaderId = leaderId

	//We register the heartbeat of the current time, to not start a new election in this node right after a new node was elected.
	if !s.isLeader {
		s.lastHeartbeat = time.Now()
	}
	s.mu.Unlock()

	if isLeader {
		log.Printf("node %d: became leader after election", idLocal)
	} else {
		log.Printf("node %d: following leader %d after election", idLocal, leaderId)
	}
}

func main() {
	fmt.Println("Starting Auction Server")

	// Flags serve as console inputs. When intializing servers, you specify the server id, and the total amount
	// of starting servers
	var (
		flagID = flag.Int("id", 0, "this node id (0..n-1)")
		flagN  = flag.Int("n", 1, "number of nodes")
	)

	flag.Parse()

	// Check if input is valid
	if *flagN <= 0 || *flagID < 0 || *flagID >= *flagN {
		log.Fatalf("bad flags: id=%d n=%d", *flagID, *flagN)
	}

	// Load addresses to other servers based on input
	addr, peers := loadPeers(*flagID, *flagN)

	// Server struct is initialized
	s := &AuctionServer{
		id:    int32(*flagID),
		addr:  addr,
		peers: peers,

		isLeader: (*flagID == 0),
		leaderId: 0,

		bidders: make(map[string]bool),
		lastBid: make(map[string]int),
		seenReq: make(map[string]bool),
	}

	if s.isLeader {
		log.Printf("node %d: starting as initial leader", s.id)
	} else {
		log.Printf("node %d: starting as follower", s.id)
	}

	// Auction timer
	go func() {

		// Cooldown before auction start with 10 second duration
		time.AfterFunc(10*time.Second, func() {
			s.mu.Lock()
			s.auctionOver = false

			if s.isLeader {
				log.Printf("node %d: Starting new auction ... Ending in 100 sec", s.id)
			}

		})

		// Start new auction after cooldown with 100 second duration
		time.AfterFunc(100*time.Second, func() {
			s.mu.Lock()
			s.auctionOver = true
			if s.isLeader {
				log.Printf("node %d: auction ended... starting new auction in 10 sek", s.id)
			}
			s.mu.Unlock()

		})

	}()

	// Network listener listening for input on own address with TCP conncetion
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterAuctionServerServer(grpcServer, s)

	log.Printf("node %d running at %s, leader=%v", s.id, s.addr, s.isLeader)

	var wg sync.WaitGroup

	//We listen
	wg.Add(1)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("serve failed: %v", err)
		}
		wg.Done()
	}()

	go func() {

		//We loop, constanty, checking whether we have become the leader and therefor
		//need to send heartbeat.
		for {
			time.Sleep(200 * time.Millisecond)
			s.mu.Lock()
			if !s.isLeader {
				s.mu.Unlock()
				continue
			}

			peersCopy := make(map[int32]string)
			for k, v := range s.peers {
				peersCopy[k] = v
			}
			s.mu.Unlock()

			//If we have become the leader, we loop through all our peers and send a heartbeat.
			for _, addr := range peersCopy {
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				cancel()
				if err != nil {
					continue
				}
				client := pb.NewAuctionServerClient(conn)
				ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
				client.HeartBeat(ctx2, &pb.Reply{})
				cancel2()
				conn.Close()
			}
		}
	}()

	//We initially check if the leader of id 0 is available. If yes we set id 0 to the new leader.
	if !s.isLeader {

		//We create loads of local variables from the global so we dont create data races.
		s.mu.Lock()
		id := s.id
		peersCopy := make(map[int32]string)
		for k, v := range s.peers {
			peersCopy[k] = v
		}
		s.mu.Unlock()

		//We get the information of the leader to make a connection.
		if addr0, ok := peersCopy[0]; ok {
			log.Printf("node %d: trying to contact initial leader 0 at %s", id, addr0)
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			conn, err := grpc.DialContext(ctx, addr0, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
			cancel()

			//if we get a connection we set the initial leader. If not we run a new election.
			if err == nil {
				conn.Close()
				s.mu.Lock()
				s.leaderId = 0
				s.isLeader = false
				s.lastHeartbeat = time.Now()
				s.mu.Unlock()
				log.Printf("node %d: following existing leader 0", id)
			} else {
				log.Printf("node %d: could not contact leader 0, starting election", id)
				s.runElection()
			}
		} else {
			log.Printf("node %d: no address for leader 0, starting election", id)
			s.runElection()
		}

		//Go function that listens for a heartbeat.
		go func() {
			for {

				//We copy the global variables
				time.Sleep(200 * time.Millisecond)
				s.mu.Lock()
				isLeader := s.isLeader
				last := s.lastHeartbeat
				s.mu.Unlock()

				//If we are the leader, then no need to listen for heartbeat.
				if isLeader {
					continue
				}

				//If sufficient time has passed and no heartbeat has been detected, we call a new election.
				if last.IsZero() || time.Since(last) > 500*time.Millisecond {
					log.Printf("node %d: heartbeat timeout, starting election", s.id)
					s.runElection()
				}
			}
		}()
	}

	wg.Wait()
}

// Load peer addresses into a map, but ignore self.
func loadPeers(selfID, n int) (string, map[int32]string) {
	addr := fmt.Sprintf("localhost:%d", basePort+selfID)

	peers := make(map[int32]string, n-1)
	for i := 0; i < n; i++ {
		if i == selfID {
			continue
		}
		peers[int32(i)] = fmt.Sprintf("localhost:%d", basePort+i)
	}
	return addr, peers
}
