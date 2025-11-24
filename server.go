package main

import (
	"context"
	"flag"
	"log"
	"sync"
)

type AuctionServer struct {
	proto.UnimplementedAuctionServerServer

	id    int
	addr  string
	peers []string

	isLeader   bool
	leaderAddr string

	mutex         sync.Mutex
	highestBid    int
	highestBidder string

	auctionOver bool
}

func (s *AuctionServer) Bid(ctx context.Context, req *proto.Amount) (*proto.Ack, error) {
}

func (s *AuctionServer) ReplicateBid(ctx context.Context, entry *proto.BidEntry) (*proto.RepAck, error) {
}

//
// Startup initialize
//
func main() {
	var (
		flagPort = flag.Int("id", 0, "this node id (0..n-1)")
	)

	// Checks for bad console inputs
	if {
		log.Fatalf("bad flags: id=%d n=%d", *flagID, *flagN)
	}

	flag.Parse()
}
