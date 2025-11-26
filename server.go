package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "github.com/gilbert400/Disys_Mandatory-Activity-5/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const basePort = 8000

type AuctionServer struct {
	pb.UnimplementedAuctionServerServer

	id    int
	addr  string
	peers []string

	// leader tracking
	isLeader      bool
	leaderAddr    string
	lastHeartbeat time.Time

	// replication ordering
	seqNo      int64
	appliedSeq int64

	// auction state
	mu            sync.Mutex
	highestBid    int
	highestBidder string
	bidders       map[string]bool
	lastBid       map[string]int
	seenReq       map[string]bool

	// time
	startTime   time.Time
	auctionOver bool
}

func (s *AuctionServer) Bid(ctx context.Context, req *pb.BidEntry) (*pb.Ack, error) {
	// if not leader -> forward
	// if leader -> validate, replicate, commit, ack

	if !s.isLeader {
		conn, err := grpc.Dial(s.leaderAddr, grpc.WithInsecure())
		if err != nil {
			s.AppointNewLeader()
			return &pb.Ack{Ack: "leader unavailable, starting election"}, nil
		}
		defer conn.Close()
		client := pb.NewAuctionServerClient(conn)

		cctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		resp, err := client.Bid(cctx, req)
		if err != nil {
			s.AppointNewLeader()
			return &pb.Ack{Ack: "no response, new election"}, nil
		}
		return resp, nil
	}

	return nil, nil
}

// 1. Called when a node detects leader failure
func (s *AuctionServer) startElection() {
	log.Printf("Node %d starting election", s.id)
	newLeaderID, newLeaderAddr := s.electNewLeader()
	s.updateLeaderState(newLeaderID, newLeaderAddr)
	s.announceNewLeader(newLeaderID, newLeaderAddr)
}

// 2. Determines which node becomes leader (highest ID)
func (s *AuctionServer) electNewLeader() (int, string) {
	highestID := s.id
	highestAddr := s.addr
	for _, peer := range s.peers {
		var peerID int
		_, err := fmt.Sscanf(peer, "localhost:%d", &peerID)
		if err != nil {
			continue
		}
		peerID = peerID - basePort
		if peerID > highestID {
			highestID = peerID
			highestAddr = peer
		}
	}
	return highestID, highestAddr
}

// 3. Updates local state and broadcasts announcement
func (s *AuctionServer) announceNewLeader(leaderID int, leaderAddr string) {
	for _, peer := range s.peers {
		conn, err := grpc.Dial(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		client := pb.NewAuctionServerClient(conn)
		client.AppointNewLeader(context.Background(), &pb.LeaderInfo{
			LeaderAddr: leaderAddr,
			LeaderId:   int32(leaderID),
		})
		conn.Close()
	}
}

func (s *AuctionServer) updateLeaderState(leaderID int, leaderAddr string) {
	s.isLeader = (s.id == leaderID)
	s.leaderAddr = leaderAddr
	log.Printf("Leader updated: node %d at %s", leaderID, leaderAddr)
}

func (s *AuctionServer) ReplicateBid(ctx context.Context, entry *pb.BidEntry) (*pb.Ack, error) {
	// apply in seq order
	return nil, nil
}

func (s *AuctionServer) forwardBidToLeader(ctx context.Context, req *pb.BidEntry) (*pb.Ack, error) {
	conn, err := grpc.Dial(s.leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return &pb.Ack{Ack: "fail: cannot reach leader"}, nil
	}
	defer conn.Close()

	client := pb.NewAuctionServerClient(conn)
	return client.Bid(ctx, req)
}

// Startup initialize
func main() {
	var (
		flagID = flag.Int("id", 0, "this node id (0..n-1)")
		flagN  = flag.Int("n", 1, "number of nodes")
	)

	flag.Parse()

	// Checks for bad console inputs
	if *flagN <= 0 || *flagID < 0 || *flagID >= *flagN {
		log.Fatalf("bad flags: id=%d n=%d", *flagID, *flagN)
	}

	// Loads list of other clients and assigns global variables
	addr, peers := loadPeers(*flagID, *flagN)

	s := &AuctionServer{
		id:    *flagID,
		addr:  addr,
		peers: peers,

		isLeader:   (*flagID == 0),
		leaderAddr: fmt.Sprintf("localhost:%d", basePort), // id 0 addr

		bidders: make(map[string]bool),
		lastBid: make(map[string]int),
		seenReq: make(map[string]bool),

		startTime: time.Now(), // leader should broadcast a real start later
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterAuctionServerServer(grpcServer, s)

	log.Printf("node %d running at %s", s.id, s.addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve failed: %v", err)
	}
}

func loadPeers(selfID, n int) (string, []string) {
	addr := fmt.Sprintf("localhost:%d", basePort+selfID)

	peers := make([]string, 0, n-1)
	for i := 0; i < n; i++ {
		if i == selfID {
			continue
		}
		peers = append(peers, fmt.Sprintf("localhost:%d", basePort+i))
	}
	return addr, peers
}
