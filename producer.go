package danube

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/danrusei/danube-go/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Producer represents a message producer that is responsible for sending
// messages to a specific topic on a message broker. It handles producer
// creation, message sending, and maintains the producer's state.
type Producer struct {
	client            *DanubeClient
	topic             string                      // name of the topic to which the producer sends messages.
	producerName      string                      // name assigned to the producer instance.
	producerID        uint64                      // The unique identifier for the producer, provided by broker
	requestID         atomic.Uint64               // atomic counter for generating unique request IDs.
	messageSequenceID atomic.Uint64               // atomic counter for generating unique message sequence IDs.
	schema            *Schema                     // The schema that defines the structure of the messages being produced.
	producerOptions   ProducerOptions             // Options that configure the behavior of the producer.
	streamClient      proto.ProducerServiceClient // gRPC client used for communication with the message broker.
	stopSignal        *atomic.Bool                // An atomic boolean signal used to indicate if the producer should stop.
}

func newProducer(
	client *DanubeClient,
	topic string,
	producerName string,
	schema *Schema,
	producerOptions ProducerOptions,
) *Producer {
	return &Producer{
		client:            client,
		topic:             topic,
		producerName:      producerName,
		producerID:        0,
		requestID:         atomic.Uint64{},
		messageSequenceID: atomic.Uint64{},
		schema:            schema,
		producerOptions:   producerOptions,
		streamClient:      nil,
		stopSignal:        &atomic.Bool{},
	}
}

// Create initializes the producer and registers it with the message broker.
//
// This method connects to the broker, sets up the producer with the provided schema,
// and starts a health check service. It handles retries in case of failures and
// updates the producerID upon successful creation.
//
// Parameters:
// - ctx: The context for managing request lifecycle and cancellation.
//
// Returns:
// - uint64: The unique ID of the created producer if successful.
// - error: An error if producer creation fails.
func (p *Producer) Create(ctx context.Context) (uint64, error) {
	// Initialize the gRPC client connection
	if err := p.connect(p.client.URI); err != nil {
		return 0, err
	}

	// Set default schema if not specified
	schema := &Schema{Name: "string_schema", TypeSchema: SchemaType_STRING}
	if p.schema != nil {
		schema = p.schema
	}

	req := &proto.ProducerRequest{
		RequestId:          p.requestID.Add(1),
		ProducerName:       p.producerName,
		TopicName:          p.topic,
		Schema:             schema.ToProto(),
		ProducerAccessMode: proto.ProducerAccessMode_Shared,
	}

	maxRetries := 4
	attempts := 0
	brokerAddr := p.client.URI

	for {
		resp, err := p.streamClient.CreateProducer(ctx, req)
		if err == nil {
			p.producerID = resp.ProducerId

			// Start health check service
			err = p.client.healthCheckService.StartHealthCheck(ctx, brokerAddr, 0, p.producerID, p.stopSignal)
			if err != nil {
				return 0, err
			}

			return p.producerID, nil
		}

		if status.Code(err) == codes.AlreadyExists {
			return 0, fmt.Errorf("producer already exists: %v", err)
		}

		attempts++
		if attempts >= maxRetries {
			return 0, fmt.Errorf("failed to create producer after retries: %v", err)
		}

		// Handle SERVICE_NOT_READY error
		if status.Code(err) == codes.Unavailable {
			time.Sleep(2 * time.Second)

			broker_addr, lookupErr := p.client.lookupService.handleLookup(ctx, brokerAddr, p.topic)
			if lookupErr != nil {
				return 0, fmt.Errorf("lookup failed: %v", lookupErr)
			}

			if err := p.connect(broker_addr); err != nil {
				return 0, err
			}
			p.client.URI = broker_addr
		} else {
			return 0, err
		}
	}
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
	// Check if the stop signal indicates that the producer should be stopped
	// this could happen due to a topic closure or movement to another broker
	if p.stopSignal.Load() {
		log.Println("Producer has been stopped, attempting to recreate.")
		if _, err := p.Create(ctx); err != nil {
			return 0, fmt.Errorf("failed to recreate producer: %v", err)
		}
	}

	// Use an empty map if attributes are nil
	if attributes == nil {
		attributes = make(map[string]string)
	}

	publishTime := uint64(time.Now().UnixNano() / int64(time.Millisecond))

	metaData := &proto.MessageMetadata{
		ProducerName: p.producerName,
		SequenceId:   p.messageSequenceID.Add(1),
		PublishTime:  publishTime,
		Attributes:   attributes,
	}

	req := &proto.MessageRequest{
		RequestId:  p.requestID.Add(1),
		ProducerId: p.producerID,
		Metadata:   metaData,
		Payload:    data,
	}

	res, err := p.streamClient.SendMessage(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("failed to send message: %v", err)
	}

	return res.SequenceId, nil
}

func (p *Producer) connect(addr string) error {
	conn, err := p.client.connectionManager.getConnection(addr, addr)
	if err != nil {
		return err
	}
	p.streamClient = proto.NewProducerServiceClient(conn.grpcConn)
	return nil
}
