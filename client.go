package danube

import (
	"context"
)

type DanubeClient struct {
	URI                string
	connectionManager  *connectionManager
	LookupService      *LookupService
	SchemaService      *SchemaService
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
		LookupService:      lookupService,
		SchemaService:      schemaService,
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
	return dc.LookupService.LookupTopic(ctx, addr, topic)
}

func (dc *DanubeClient) GetSchema(ctx context.Context, topic string) (*Schema, error) {
	return dc.SchemaService.GetSchema(ctx, dc.URI, topic)
}

type DanubeClientBuilder struct {
	URI               string
	ConnectionOptions []DialOption
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
