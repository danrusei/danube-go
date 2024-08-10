package danube

import (
	"context"
)

// DanubeClient is the main client for interacting with the Danube messaging system.
// It provides methods to create producers and consumers, perform topic lookups, and retrieve schema information.
type DanubeClient struct {
	URI                string
	connectionManager  *connectionManager
	lookupService      *lookupService
	schemaService      *schemaService
	healthCheckService *healthCheckService
}

// NewClient initializes a new DanubeClientBuilder. The builder pattern allows for configuring and constructing
// a DanubeClient instance with optional settings and options.
//
// Returns:
// - *DanubeClientBuilder: A new instance of DanubeClientBuilder for configuring and building a DanubeClient.
func NewClient() *DanubeClientBuilder {
	return &DanubeClientBuilder{}
}

func newDanubeClient(builder DanubeClientBuilder) *DanubeClient {
	connectionManager := newConnectionManager(builder.ConnectionOptions)
	lookupService := NewLookupService(connectionManager)
	schemaService := newSchemaService(connectionManager)
	healthCheckService := newHealthCheckService(connectionManager)

	return &DanubeClient{
		URI:                builder.URI,
		connectionManager:  connectionManager,
		lookupService:      lookupService,
		schemaService:      schemaService,
		healthCheckService: healthCheckService,
	}
}

// NewProducer returns a new ProducerBuilder, which is used to configure and create a Producer instance.
//
// Parameters:
// - ctx: The context for managing the lifecycle of the ProducerBuilder and any operations performed with it.
func (dc *DanubeClient) NewProducer(ctx context.Context) *ProducerBuilder {
	return newProducerBuilder(dc)
}

// NewConsumer returns a new ConsumerBuilder, which is used to configure and create a Consumer instance.
//
// Parameters:
// - ctx: The context for managing the lifecycle of the ConsumerBuilder and any operations performed with it.
func (dc *DanubeClient) NewConsumer(ctx context.Context) *ConsumerBuilder {
	return newConsumerBuilder(dc)
}

// LookupTopic retrieves the address of the broker responsible for a specified topic.
//
// Parameters:
// - ctx: The context for managing the lookup operation.
// - addr: The address of the lookup service.
// - topic: The name of the topic to look up.
//
// Returns:
// - *LookupResult: The result of the lookup operation, containing broker address and other details.
// - error: An error if the lookup fails or other issues occur.
func (dc *DanubeClient) LookupTopic(ctx context.Context, addr string, topic string) (*LookupResult, error) {
	return dc.lookupService.lookupTopic(ctx, addr, topic)
}

// GetSchema retrieves the schema associated with a specified topic from the schema service.
//
// Parameters:
// - ctx: The context for managing the schema retrieval operation.
// - topic: The name of the topic for which the schema is to be retrieved.
//
// Returns:
// - *Schema: The schema associated with the topic.
// - error: An error if the schema retrieval fails or other issues occur.
func (dc *DanubeClient) GetSchema(ctx context.Context, topic string) (*Schema, error) {
	return dc.schemaService.getSchema(ctx, dc.URI, topic)
}

// DanubeClientBuilder is used for configuring and creating a DanubeClient instance. It provides methods for setting
// various options, including the service URL, connection options, and logger.
//
// Fields:
// - URI: The base URI for the Danube service. This is required for constructing the client.
// - ConnectionOptions: Optional connection settings for configuring how the client connects to the service.
type DanubeClientBuilder struct {
	URI               string
	ConnectionOptions []DialOption
}

// ServiceURL sets the base URI for the Danube service in the builder.
//
// Parameters:
// - url: The base URI to use for connecting to the Danube service.
//
// Returns:
// - *DanubeClientBuilder: The updated builder instance with the new service URL.
func (b *DanubeClientBuilder) ServiceURL(url string) *DanubeClientBuilder {
	b.URI = url
	return b
}

// WithConnectionOptions sets optional connection settings for the client in the builder.
//
// Parameters:
// - options: A slice of DialOption used to configure the client's connection settings.
//
// Returns:
// - *DanubeClientBuilder: The updated builder instance with the specified connection options.
func (b *DanubeClientBuilder) WithConnectionOptions(options []DialOption) *DanubeClientBuilder {
	b.ConnectionOptions = options
	return b
}

// Build constructs and returns a DanubeClient instance based on the configuration specified in the builder.
//
// Returns:
// - *DanubeClient: A new instance of DanubeClient configured with the specified options.
func (b *DanubeClientBuilder) Build() *DanubeClient {
	return newDanubeClient(*b)
}
