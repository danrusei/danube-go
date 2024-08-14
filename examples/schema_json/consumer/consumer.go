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

	client := danube.NewClient().ServiceURL("127.0.0.1:6650").Build()

	ctx := context.Background()
	topic := "/default/test_topic"
	consumerName := "test_consumer"
	subscriptionName := "test_subscription"
	subType := danube.Exclusive

	consumer, err := client.NewConsumer(ctx).
		WithConsumerName(consumerName).
		WithTopic(topic).
		WithSubscription(subscriptionName).
		WithSubscriptionType(subType).
		Build()
	if err != nil {
		log.Fatalf("Failed to initialize the consumer: %v", err)
	}

	if err := consumer.Subscribe(ctx); err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	log.Printf("The Consumer %s was created", consumerName)

	// Receiving messages
	stream, err := consumer.Receive(ctx)
	if err != nil {
		log.Fatalf("Failed to receive messages: %v", err)
	}

	for msg := range stream {

		var myMessage MyMessage
		if err := json.Unmarshal(msg.GetPayload(), &myMessage); err != nil {
			log.Printf("Failed to decode message: %v", err)
		} else {
			fmt.Printf("Received message: %+v\n", myMessage)
		}
	}
}
