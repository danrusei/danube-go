package main

import (
	"context"
	"fmt"
	"log"

	"github.com/danrusei/danube-go"
)

func main() {
	// Setup logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	client := danube.NewClient().ServiceURL("127.0.0.1:6650").Build()

	ctx := context.Background()
	topic := "/default/test_topic"
	subType := danube.Exclusive

	consumer, err := client.NewConsumer(ctx).
		WithConsumerName("test_consumer").
		WithTopic(topic).
		WithSubscription("test_subscription").
		WithSubscriptionType(subType).
		Build()
	if err != nil {
		log.Fatalf("Failed to initialize the consumer: %v", err)
	}

	consumerID, err := consumer.Subscribe(ctx)
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}
	log.Printf("The Consumer with ID: %v was created", consumerID)

	// Receiving messages
	streamClient, err := consumer.Receive(ctx)
	if err != nil {
		log.Fatalf("Failed to receive messages: %v", err)
	}

	for {
		msg, err := streamClient.Recv()
		if err != nil {
			log.Fatalf("Error receiving message: %v", err)
			break
		}

		fmt.Printf("Received message: %+v\n", string(msg.GetPayload()))

	}
}
