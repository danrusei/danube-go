package danube

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Producer struct {
	client            *DanubeClient
	topic             string
	producerName      string
	producerID        *uint64
	requestID         atomic.Uint64
	messageSequenceID atomic.Uint64
	schema            *Schema
	producerOptions   ProducerOptions
	streamClient      ProducerServiceClient
	stopSignal        *atomic.Bool
}

func NewProducer(
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
		producerID:        nil,
		requestID:         atomic.Uint64{},
		messageSequenceID: atomic.Uint64{},
		schema:            schema,
		producerOptions:   producerOptions,
		streamClient:      nil,
		stopSignal:        atomic.NewBool(false),
	}
}

func (p *Producer) Create(ctx context.Context) (uint64, error) {
	// Initialize the gRPC client connection
	if err := p.connect(ctx, p.client.URI); err != nil {
		return 0, err
	}

	// Set default schema if not specified
	schema := &Schema{Name: "bytes_schema", Type: SchemaType_STRING}
	if p.schema != nil {
		schema = p.schema
	}

	req := &ProducerRequest{
		RequestId:          p.requestID.Add(1),
		ProducerName:       p.producerName,
		TopicName:          p.topic,
		Schema:             schema.ToProto(),
		ProducerAccessMode: ProducerAccessMode_SHARED,
	}

	maxRetries := 4
	attempts := 0
	brokerAddr := p.client.URI

	for {
		res, err := p.streamClient.CreateProducer(ctx, req)
		if err == nil {
			p.producerID = &res.ProducerId

			// Start health check service
			stopSignal := p.stopSignal
			go func() {
				_ = p.client.healthCheckService.StartHealthCheck(ctx, brokerAddr, 0, *p.producerID, stopSignal)
			}()
			return *p.producerID, nil
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

			addr, lookupErr := p.client.lookupService.HandleLookup(ctx, p.topic)
			if lookupErr != nil {
				return 0, fmt.Errorf("lookup failed: %v", lookupErr)
			}

			brokerAddr = addr
			if err := p.connect(ctx, brokerAddr); err != nil {
				return 0, err
			}
			p.client.URI = brokerAddr
		} else {
			return 0, err
		}
	}
}

func (p *Producer) Send(ctx context.Context, data []byte) (uint64, error) {
	publishTime := uint64(time.Now().UnixNano() / int64(time.Millisecond))

	metaData := &MessageMetadata{
		ProducerName: p.producerName,
		SequenceId:   p.messageSequenceID.Add(1),
		PublishTime:  publishTime,
	}

	sendMessage := &SendMessage{
		RequestId:  p.requestID.Add(1),
		ProducerId: *p.producerID,
		Metadata:   metaData,
		Message:    data,
	}

	req := &MessageRequest{
		Message: sendMessage.ToProto(),
	}

	res, err := p.streamClient.SendMessage(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("failed to send message: %v", err)
	}

	return res.SequenceId, nil
}

func (p *Producer) connect(ctx context.Context, addr string) error {
	conn, err := p.client.cnxManager.GetConnection(ctx, addr, addr)
	if err != nil {
		return err
	}
	p.streamClient = NewProducerServiceClient(conn.grpcConn)
	return nil
}
