package danube

import (
	"context"
	"log"
)

type DanubeClient struct {
	URI                string
	connectionManager  *connectionManager
	lookupService      *LookupService
	schemaService      *SchemaService
	healthCheckService *healthCheckService
}

func NewClient() *DanubeClientBuilder {
	return &DanubeClientBuilder{}
}

func newDanubeClient(builder DanubeClientBuilder) *DanubeClient {
	connectionManager := newConnectionManager(builder.ConnectionOptions)
	lookupService := NewLookupService(connectionManager)
	schemaService := NewSchemaService(connectionManager)
	healthCheckService := newHealthCheckService(connectionManager)

	return &DanubeClient{
		URI:                builder.URI,
		connectionManager:  connectionManager,
		lookupService:      lookupService,
		schemaService:      schemaService,
		healthCheckService: healthCheckService,
	}
}

func (dc *DanubeClient) NewProducer(ctx context.Context) *ProducerBuilder {
	return newProducerBuilder(dc)
}

func (dc *DanubeClient) NewConsumer(ctx context.Context) *ConsumerBuilder {
	return newConsumerBuilder(dc)
}

func (dc *DanubeClient) LookupTopic(ctx context.Context, addr string, topic string) (*LookupResult, error) {
	return dc.lookupService.LookupTopic(ctx, addr, topic)
}

func (dc *DanubeClient) GetSchema(ctx context.Context, topic string) (*Schema, error) {
	return dc.schemaService.GetSchema(ctx, dc.URI, topic)
}

type DanubeClientBuilder struct {
	URI               string
	ConnectionOptions []DialOption
	Logger            *log.Logger
}

func (b *DanubeClientBuilder) ServiceURL(url string) *DanubeClientBuilder {
	b.URI = url
	return b
}

func (b *DanubeClientBuilder) WithConnectionOptions(options []DialOption) *DanubeClientBuilder {
	b.ConnectionOptions = options
	return b
}

func (b *DanubeClientBuilder) Build() *DanubeClient {
	return newDanubeClient(*b)
}
