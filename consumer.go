package danube

import (
	"context"
	"fmt"
	"sync"

	"github.com/danrusei/danube-go/proto"
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
	mu               sync.Mutex
	client           *DanubeClient
	topicName        string          // the name of the topic that the consumer subscribes to
	consumerName     string          // the name assigned to the consumer instance
	consumers        []topicConsumer // the under the hood consumers for partitioned topics
	subscription     string          // the name of the subscription for the consumer
	subscriptionType SubType         // the type of subscription (e.g., EXCLUSIVE, SHARED, FAILOVER)
	consumerOptions  ConsumerOptions // configuration options for the consumer
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
	}
}

// Subscribe initializes the subscription to the non-partitioned or partitioned topic and starts the health check service.
// It establishes a gRPC connection with the brokers and requests to subscribe to the topic.
//
// Parameters:
// - ctx: The context for managing the subscription lifecycle.
//
// Returns:
// - error: An error if the subscription fails or if initialization encounters issues.
func (c *Consumer) Subscribe(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get topic partitions
	partitions, err := c.client.lookupService.topicPartitions(ctx, c.client.URI, c.topicName)
	if err != nil {
		return err
	}

	// Channels to collect errors and results
	errChan := make(chan error, len(partitions))
	doneChan := make(chan struct{}, len(partitions))

	// Create and subscribe to topicConsumer
	var consumers []topicConsumer
	for _, partition := range partitions {
		partition := partition
		go func() {
			defer func() { doneChan <- struct{}{} }()

			tc := newTopicConsumer(
				c.client,
				partition,
				c.consumerName,
				c.subscription,
				&c.subscriptionType,
				c.consumerOptions,
			)

			// Subscribe the topicConsumer and handle result
			if _, err := tc.subscribe(ctx); err != nil {
				errChan <- err
				return
			}

			consumers = append(consumers, tc)
		}()
	}

	// Wait for all goroutines to complete and check for errors
	for i := 0; i < len(partitions); i++ {
		select {
		case err := <-errChan:
			return err
		case <-doneChan:
			// Successful completion
		}
	}

	if len(consumers) == 0 {
		return fmt.Errorf("no partitions found")
	}

	c.consumers = consumers

	return nil
}

// Receive starts receiving messages from the subscribed partitioned or non-partitioned topic.
// It continuously polls for new messages and handles them as long as the stopSignal has not been set to true.
//
// Parameters:
// - ctx: The context for managing the receive operation.
//
// Returns:
// - StreamMessage channel for receiving messages from the broker.
// - error: An error if the receive client cannot be created or if other issues occur.
func (c *Consumer) Receive(ctx context.Context) (chan *proto.StreamMessage, error) {
	// Create a channel to send messages to the client
	msgChan := make(chan *proto.StreamMessage, 100) // Buffer size of 100, adjust as needed

	var wg sync.WaitGroup

	// Spawn a goroutine for each topicConsumer
	for _, consumer := range c.consumers {
		consumer := consumer // capture loop variable
		wg.Add(1)

		go func() {
			defer wg.Done()
			stream, err := consumer.receive(ctx)
			if err != nil {
				fmt.Println("Error receiving messages:", err)
				return
			}

			// Receive messages from the stream
			for {
				select {
				case <-ctx.Done():
					// Context canceled, stop receiving messages
					return
				default:
					message, err := stream.Recv()
					if err != nil {
						// Error receiving message, log it and exit the loop
						fmt.Println("Error receiving message:", err)
						return
					}

					// Send the received message to the channel
					select {
					case msgChan <- message:
					case <-ctx.Done():
						// Context canceled, stop sending messages
						return
					}
				}
			}
		}()
	}

	// Close the message channel when all goroutines are done
	go func() {
		wg.Wait()
		close(msgChan)
	}()

	return msgChan, nil
}
