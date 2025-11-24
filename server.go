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
	mutex         sync.Mutex
	highestBid    int
	highestBidder string
	bidders       map[string]bool
	lastBid       map[string]int
	seenReq       map[string]bool

	// time
	startTime   time.Time
	auctionOver bool
}

func (s *AuctionServer) Bid(ctx context.Context, req *pb.Amount) (*pb.Ack, error) {
	// if not leader -> forward
	// if leader -> validate, replicate, commit, ack
	return nil, nil
}

func (s *AuctionServer) ReplicateBid(ctx context.Context, entry *pb.BidEntry) (*pb.RepAck, error) {
	// apply in seq order
	return nil, nil
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
