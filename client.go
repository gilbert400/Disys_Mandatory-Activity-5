package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	pb "disys_chitchat_activity03/grpc"
	"google.golang.org/grpc"
)

var clock int32

// Max function to return the biggest number.
func max(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func main() {
	//Connect to grpc server
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}

	//Ensure connection closes in the end.
	defer conn.Close()

	//create new client and get chat stream for sending and receiving messages.
	client := pb.NewChitChatClient(conn)
	stream, err := client.SendMessage(context.Background())
	if err != nil {
		log.Fatalf("Error creating stream: %v", err)
	}

	//Get client name. Will act as the clients ID.
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter your name: ")
	clientName, _ := reader.ReadString('\n')
	clientName = strings.TrimSpace(clientName)

	//Increment clock for the joining. Send a dummy message that indicates the user has joined.
	clock++
	stream.Send(&pb.MessageLog{
		Timestamp:     clock,
		ComponentName: "Client",
		EventType:     "Client",
		ClientName:    clientName,
		HasJoined:     false,
	})

	//A goroutine that is in a "while true" loop, checking if we receive any messages.
	go func() {
		for {
			//Check receival of message.
			inMsg, err := stream.Recv()
			//If the connection to the server is lost, stop the client side program aswell.
			if err != nil {
				clock++
				log.Printf("[Client @ %d] Disconnected from server.", clock)
				os.Exit(0)
			}

			//Use Chandy–Lamport algorithm used to increment lamport clock of client.
			clock = max(clock, inMsg.Timestamp) + 1

			//Printing of message received from server.
			fmt.Printf("[%s @ %d] %s: %s\n", inMsg.ComponentName, clock, inMsg.ClientName, inMsg.EventType)
		}
	}()

	//A "while true" loop checking if the user has typed in a message in the CLI.
	for {
		text, _ := reader.ReadString('\n')
		text = strings.TrimSpace(text)
		//Simple exit command to stop the client.
		if text == "exit" {
			clock++
			log.Printf("[Client @ %d] Leaving chat...", clock)
			break
		}

		//We increment the clock, and send a new message to the server with the content of the clients CLI input.
		clock++
		msg := &pb.MessageLog{
			Timestamp:     clock,
			ComponentName: "Client",
			EventType:     text,
			ClientName:    clientName,
			HasJoined:     true,
		}

		if err := stream.Send(msg); err != nil {
			log.Fatalf("Error sending message: %v", err)
		}
	}

	//If we reach here, the user has broken the loop typing exit, and we end the stream. Our
	stream.CloseSend()

	//Our defer from earlier also calls the connection to close in the end.
}
