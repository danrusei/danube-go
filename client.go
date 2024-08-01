package danube

import (
	"context"
)

type DanubeClient struct {
	URI                string
	ConnectionManager  *ConnectionManager
	LookupService      *LookupService
	SchemaService      *SchemaService
	HealthCheckService *HealthCheckService
}

func NewDanubeClient(builder DanubeClientBuilder) (*DanubeClient, error) {
	connectionManager := NewConnectionManager(builder.ConnectionOptions)
	lookupService := NewLookupService(connectionManager)
	schemaService := NewSchemaService(connectionManager)
	healthCheckService := NewHealthCheckService(connectionManager)

	return &DanubeClient{
		URI:                builder.URI,
		ConnectionManager:  connectionManager,
		LookupService:      lookupService,
		SchemaService:      schemaService,
		HealthCheckService: healthCheckService,
	}, nil
}

func (dc *DanubeClient) NewProducer() *ProducerBuilder {
	return NewProducerBuilder(dc)
}

func (dc *DanubeClient) NewConsumer() *ConsumerBuilder {
	return NewConsumerBuilder(dc)
}

func (dc *DanubeClient) LookupTopic(ctx context.Context, addr string, topic string) (*LookupResult, error) {
	return dc.LookupService.LookupTopic(ctx, addr, topic)
}

func (dc *DanubeClient) GetSchema(ctx context.Context, topic string) (*Schema, error) {
	return dc.SchemaService.GetSchema(ctx, dc.URI, topic)
}

type DanubeClientBuilder struct {
	URI               string
	ConnectionOptions ConnectionOptions
}

func (b *DanubeClientBuilder) ServiceURL(url string) *DanubeClientBuilder {
	b.URI = url
	return b
}

func (b *DanubeClientBuilder) WithConnectionOptions(options ConnectionOptions) *DanubeClientBuilder {
	b.ConnectionOptions = options
	return b
}

func (b *DanubeClientBuilder) Build() (*DanubeClient, error) {
	return NewDanubeClient(*b)
}