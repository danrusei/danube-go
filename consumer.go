package danube

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/danrusei/danube-go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// the type of subscription (e.g., EXCLUSIVE, SHARED, FAILOVER)
type SubType int

const (
	// Exclusive - only one consumer can subscribe to a specific subscription
	Exclusive SubType = iota
	//  Shared - multiple consumers can subscribe, messages are delivered round-robin
	Shared
	// FailOver - similar to exclusive subscriptions, but multiple consumers can subscribe, and one actively receives messages
	FailOver
)

// Consumer represents a message consumer that subscribes to a topic and receives messages.
// It handles communication with the message broker and manages the consumer's state.
type Consumer struct {
	client           *DanubeClient
	topicName        string                      // the name of the topic that the consumer subscribes to
	consumerName     string                      // the name assigned to the consumer instance
	consumerID       uint64                      // the unique identifier of the consumer assigned by the broker after subscription
	subscription     string                      // the name of the subscription for the consumer
	subscriptionType SubType                     // the type of subscription (e.g., EXCLUSIVE, SHARED, FAILOVER)
	consumerOptions  ConsumerOptions             // configuration options for the consumer
	requestID        atomic.Uint64               // atomic counter for generating unique request IDs
	streamClient     proto.ConsumerServiceClient // the gRPC client used to communicate with the consumer service
	stopSignal       *atomic.Bool                // atomic boolean flag to indicate if the consumer should be stopped
}

func newConsumer(
	client *DanubeClient,
	topicName, consumerName, subscription string,
	subType *SubType,
	options ConsumerOptions,
) *Consumer {
	var subscriptionType SubType
	if subType != nil {
		subscriptionType = *subType
	} else {
		subscriptionType = Shared
	}

	return &Consumer{
		client:           client,
		topicName:        topicName,
		consumerName:     consumerName,
		subscription:     subscription,
		subscriptionType: subscriptionType,
		consumerOptions:  options,
		stopSignal:       &atomic.Bool{},
	}
}

// Subscribe initializes the subscription to the topic and starts the health check service.
// It establishes a gRPC connection with the broker and requests to subscribe to the topic.
//
// Parameters:
// - ctx: The context for managing the subscription lifecycle.
//
// Returns:
// - uint64: The unique identifier assigned to the consumer by the broker.
// - error: An error if the subscription fails or if initialization encounters issues.
func (c *Consumer) Subscribe(ctx context.Context) (uint64, error) {
	brokerAddr, err := c.client.lookupService.HandleLookup(ctx, c.client.URI, c.topicName)
	if err != nil {
		return 0, err
	}

	conn, err := grpc.NewClient(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return 0, err
	}

	c.streamClient = proto.NewConsumerServiceClient(conn)

	req := &proto.ConsumerRequest{
		RequestId:        c.requestID.Add(1),
		TopicName:        c.topicName,
		ConsumerName:     c.consumerName,
		Subscription:     c.subscription,
		SubscriptionType: proto.ConsumerRequest_SubscriptionType(c.subscriptionType),
	}

	resp, err := c.streamClient.Subscribe(ctx, req)
	if err != nil {
		return 0, err
	}

	c.consumerID = resp.GetConsumerId()

	// Start health check service
	err = c.client.healthCheckService.StartHealthCheck(ctx, brokerAddr, 1, c.consumerID, c.stopSignal)
	if err != nil {
		return 0, err
	}

	return c.consumerID, nil
}

// Receive starts receiving messages from the subscribed topic. It continuously polls for new messages
// and handles them as long as the stopSignal has not been set to true.
//
// Parameters:
// - ctx: The context for managing the receive operation.
//
// Returns:
// - proto.ConsumerService_ReceiveMessagesClient: A client for receiving messages from the broker.
// - error: An error if the receive client cannot be created or if other issues occur.
func (c *Consumer) Receive(ctx context.Context) (proto.ConsumerService_ReceiveMessagesClient, error) {
	if c.streamClient == nil {
		return nil, errors.New("stream client not initialized")
	}

	req := &proto.ReceiveRequest{
		RequestId:  c.requestID.Add(1),
		ConsumerId: c.consumerID,
	}

	// Check if stopSignal is set
	if c.stopSignal.Load() {
		log.Println("Consumer has been stopped by broker, attempting to resubscribe.")

		maxRetries := 3
		attempts := 0
		var err error

		for attempts < maxRetries {
			if _, err = c.Subscribe(ctx); err == nil {
				log.Println("Successfully resubscribed.")
				return c.streamClient.ReceiveMessages(ctx, req)
			}

			attempts++
			log.Printf("Resubscription attempt %d/%d failed: %v", attempts, maxRetries, err)
			time.Sleep(2 * time.Second) // Wait before retrying
		}

		return nil, fmt.Errorf("failed to resubscribe after %d attempts: %v", maxRetries, err)
	}

	return c.streamClient.ReceiveMessages(ctx, req)
}
