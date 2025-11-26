package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/gilbert400/Disys_Mandatory-Activity-5/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const clientBasePort = 8000
const maxNodes = 4

// Main client setup and control
func main() {
	rand.Seed(time.Now().UnixNano())

	// Console input from initialization
	bidderFlag := flag.String("bidder", "anonymous", "bidder name")
	flag.Parse()

	conn, err := connectToAnyNode()
	if err != nil {
		fmt.Println("could not connect to any server:", err)
		return
	}
	defer conn.Close()

	client := pb.NewAuctionServerClient(conn)
	reader := bufio.NewScanner(os.Stdin)

	// Console commands representation
	fmt.Println("Commands:")
	fmt.Println("  bid <amount>")
	fmt.Println("  result")
	fmt.Println("  exit")

	// Client console control input
	for {
		fmt.Print("> ")
		if !reader.Scan() {
			break
		}
		line := strings.TrimSpace(reader.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		switch strings.ToLower(parts[0]) {
		case "exit", "quit":
			return
		case "bid":
			if len(parts) != 2 {
				fmt.Println("usage: bid <amount>")
				continue
			}
			amount, err := strconv.Atoi(parts[1])
			if err != nil {
				fmt.Println("invalid amount")
				continue
			}
			doBid(client, *bidderFlag, amount)
		case "result":
			doResult(client)
		default:
			fmt.Println("unknown command")
		}
	}
}

// Simulate that we connect to a random server when requesting
func connectToAnyNode() (*grpc.ClientConn, error) {
	excluded := []int{}
	for {
		if len(excluded) >= maxNodes {
			return nil, fmt.Errorf("no reachable nodes")
		}
		port := randomPort(excluded)
		addr := fmt.Sprintf("localhost:%d", port)

		conn, err := grpc.Dial(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		if err != nil {
			excluded = append(excluded, port)
			continue
		}
		return conn, nil
	}
}

// Return a random port while excluding "crashed" or certain servers.
func randomPort(excluded []int) int {
	for {
		port := clientBasePort + rand.Intn(maxNodes)
		skip := false
		for _, p := range excluded {
			if p == port {
				skip = true
				break
			}
		}
		if !skip {
			return port
		}
	}
}

// Call bid function on arbitrary server
func doBid(client pb.AuctionServerClient, bidder string, amount int) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := client.Bid(ctx, &pb.BidEntry{
		Bidder: bidder,
		Amount: int32(amount),
	})
	if err != nil {
		fmt.Println("ack: exception:", err)
		return
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	outcome, err := client.Result(ctx2, &pb.Reply{})
	if err != nil {
		fmt.Println("ack: exception:", err)
		return
	}

	// Check if acknowledgement was recieved, and bid was accepted
	if outcome.GetHighestBid() == int32(amount) && outcome.GetHighestBidder() == bidder {
		fmt.Println("ack: success")
	} else {
		fmt.Println("ack: fail")
	}
}

func doResult(client pb.AuctionServerClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	outcome, err := client.Result(ctx, &pb.Reply{})
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	fmt.Printf("result: %s\n", outcome.GetResult())
	fmt.Printf("highest bid: %d\n", outcome.GetHighestBid())
	fmt.Printf("highest bidder: %s\n", outcome.GetHighestBidder())
}
