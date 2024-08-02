package danube

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/danrusei/danube-go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type SubType int

const (
	Exclusive SubType = iota
	Shared
	FailOver
)

type Consumer struct {
	client           *DanubeClient
	topicName        string
	consumerName     string
	consumerID       *uint64
	subscription     string
	subscriptionType SubType
	consumerOptions  ConsumerOptions
	requestID        atomic.Uint64
	streamClient     proto.ConsumerServiceClient
	stopSignal       *atomic.Bool
}

func NewConsumer(
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

func (c *Consumer) Subscribe(ctx context.Context) (uint64, error) {
	brokerAddr, err := c.client.LookupService.HandleLookup(ctx, c.client.URI, c.topicName)
	if err != nil {
		return 0, err
	}

	conn, err := grpc.NewClient(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return 0, err
	}
	defer conn.Close()

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

	// Start health check service
	stopSignal := c.stopSignal
	go func() {
		_ = c.client.HealthCheckService.StartHealthCheck(ctx, brokerAddr, 0, *c.consumerID, stopSignal)
	}()

	c.consumerID = &resp.ConsumerId
	return resp.ConsumerId, nil
}

func (c *Consumer) Receive(ctx context.Context) (proto.ConsumerService_ReceiveMessagesClient, error) {
	if c.streamClient == nil {
		return nil, errors.New("stream client not initialized")
	}

	req := &proto.ReceiveRequest{
		RequestId:  c.requestID.Add(1),
		ConsumerId: *c.consumerID,
	}

	return c.streamClient.ReceiveMessages(ctx, req)
}
