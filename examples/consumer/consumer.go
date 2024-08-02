package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/danrusei/danube-go"
)

type MyMessage struct {
	Field1 string `json:"field1"`
	Field2 int    `json:"field2"`
}

func main() {
	// Setup logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	clientBuilder := &danube.DanubeClientBuilder{}
	clientBuilder.ServiceURL("http://[::1]:6650")
	client, err := clientBuilder.Build()
	if err != nil {
		log.Fatalf("Failed to create Danube client: %v", err)
	}

	topic := "/default/test_topic"
	consumerOptions := danube.ConsumerOptions{} // Customize options as needed
	subType := danube.Exclusive

	consumer := danube.NewConsumer(client, topic, "test_consumer", "test_subscription", &subType, consumerOptions)

	consumerID, err := consumer.Subscribe(context.Background())
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}
	log.Printf("The Consumer with ID: %v was created", consumerID)

	// Receiving messages
	streamClient, err := consumer.Receive(context.Background())
	if err != nil {
		log.Fatalf("Failed to receive messages: %v", err)
	}

	for {
		msg, err := streamClient.Recv()
		if err != nil {
			log.Fatalf("Error receiving message: %v", err)
			break
		}

		var myMessage MyMessage
		if err := json.Unmarshal(msg.GetMessages(), &myMessage); err != nil {
			log.Printf("Failed to decode message: %v", err)
		} else {
			fmt.Printf("Received message: %+v\n", myMessage)
		}
	}
}
