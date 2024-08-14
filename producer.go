package danube

import (
	"context"
	"fmt"
	"sync"
)

// Producer represents a message producer that is responsible for sending
// messages to a specific partitioned or non-partitioned topic on a message broker.
// It handles producer creation, message sending, and maintains the producer's state.
type Producer struct {
	mu              sync.Mutex
	client          *DanubeClient
	topicName       string          // name of the topic to which the producer sends messages.
	schema          *Schema         // The schema that defines the structure of the messages being produced.
	producerName    string          // name assigned to the producer instance.
	partitions      int32           // The number of partitions for the topic
	messageRouter   *MessageRouter  // the way the messages will be delivered to consumers
	producers       []topicProducer // all the underhood producers, for sending messages to topic partitions
	producerOptions ProducerOptions // Options that configure the behavior of the producer.
}

func newProducer(
	client *DanubeClient,
	topicName string,
	producerName string,
	schema *Schema,
	producerOptions ProducerOptions,
) *Producer {

	// Set default schema if not specified
	if schema == nil {
		schema = &Schema{Name: "string_schema", TypeSchema: SchemaType_STRING}
	}

	return &Producer{
		client:          client,
		topicName:       topicName,
		schema:          schema,
		producerName:    producerName,
		partitions:      0, // Default to 0 for non-partitioned
		messageRouter:   nil,
		producerOptions: producerOptions,
	}
}

// Create initializes the producer and registers it with the message brokers.
//
// Parameters:
// - ctx: The context for managing request lifecycle and cancellation.
//
// Returns:
// - error: An error if producer creation fails.
func (p *Producer) Create(ctx context.Context) error {

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.partitions == 0 {
		// Create a single TopicProducer for non-partitioned topic
		topicProducer := newTopicProducer(
			p.client,
			p.topicName,
			p.producerName,
			p.schema,
			p.producerOptions,
		)

		_, err := topicProducer.create(ctx)
		if err != nil {
			return err
		}

		p.producers = append(p.producers, topicProducer)

	} else {
		if p.messageRouter == nil {
			p.messageRouter = &MessageRouter{partitions: p.partitions}
		}

		producers := make([]topicProducer, p.partitions)
		errChan := make(chan error, p.partitions)
		doneChan := make(chan struct{}, p.partitions)

		for partitionID := int32(0); partitionID < p.partitions; partitionID++ {
			go func(partitionID int32) {
				defer func() { doneChan <- struct{}{} }()

				// Generate the topic string with partition ID
				topicName := fmt.Sprintf("%s-part-%d", p.topicName, partitionID)

				// Generate the producer name with partition ID
				producerName := fmt.Sprintf("%s-%d", p.producerName, partitionID)

				// Create a new TopicProducer instance
				topicProducer := newTopicProducer(
					p.client,
					topicName,
					producerName,
					p.schema,
					p.producerOptions,
				)

				// Create the topic producer
				_, err := topicProducer.create(ctx)
				if err != nil {
					errChan <- err
					return
				}

				producers[partitionID] = topicProducer
			}(partitionID)
		}

		// Wait for all goroutines to finish and check for errors
		for i := int32(0); i < p.partitions; i++ {
			select {
			case err := <-errChan:
				// If any error occurs, return immediately
				return err
			case <-doneChan:
				// Process completion
			}
		}

		p.producers = producers
	}

	return nil
}

// Send sends a message to the topic associated with this producer.
//
// It constructs a message request and sends it to the broker. The method handles
// payload and error reporting. It assumes that the producer has been successfully
// created and is ready to send messages.
//
// Parameters:
// - ctx: The context for managing request lifecycle and cancellation.
// - data: The message payload to be sent.
// - attributes: user-defined properties or attributes associated with the message
//
// Returns:
// - uint64: The sequence ID of the sent message if successful.
// - error: An error if message sending fail
func (p *Producer) Send(ctx context.Context, data []byte, attributes map[string]string) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var partitionID int32
	if p.partitions > 0 && p.messageRouter != nil {
		// Use message router for partitioned topics
		partitionID = p.messageRouter.RoundRobin()
	} else {
		partitionID = 0
	}

	if partitionID >= int32(len(p.producers)) {
		return 0, fmt.Errorf("partition ID out of range")
	}

	requestID, err := p.producers[partitionID].send(ctx, data, attributes)
	if err != nil {
		fmt.Printf("Unable to send the data to producer: %s", p.producers[partitionID].producerName)
	}

	return requestID, nil
}
