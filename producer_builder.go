package danube

import "fmt"

// ProducerBuilder is a builder for creating a new Producer instance. It allows
// setting various properties for the producer such as topic, name, schema, and options.
type ProducerBuilder struct {
	client          *DanubeClient
	topic           string
	producerName    string
	schema          *Schema
	producerOptions ProducerOptions
}

func newProducerBuilder(client *DanubeClient) *ProducerBuilder {
	return &ProducerBuilder{
		client:          client,
		topic:           "",
		producerName:    "",
		schema:          nil,
		producerOptions: ProducerOptions{},
	}
}

// WithTopic sets the topic name for the producer. This is a required field.
//
// Parameters:
// - topic: The name of the topic for the producer.
func (pb *ProducerBuilder) WithTopic(topic string) *ProducerBuilder {
	pb.topic = topic
	return pb
}

// WithName sets the name of the producer. This is a required field.
//
// Parameters:
// - producerName: The name assigned to the producer instance.
func (pb *ProducerBuilder) WithName(producerName string) *ProducerBuilder {
	pb.producerName = producerName
	return pb
}

// WithSchema sets the schema for the producer, defining the structure of the messages.
//
// Parameters:
// - schemaName: The name of the schema.
// - schemaType: The type of the schema (e.g., SchemaType_BYTES, SchemaType_STRING, SchemaType_JSON)
// - schemaData: The data or definition of the schema only if it is SchemaType_JSON
func (pb *ProducerBuilder) WithSchema(schemaName string, schemaType SchemaType, schemaData string) *ProducerBuilder {
	pb.schema = NewSchema(schemaName, schemaType, schemaData)
	return pb
}

// WithOptions sets the configuration options for the producer. This allows for customization
// of producer behavior.
//
// Parameters:
// - options: Configuration options for the producer.
func (pb *ProducerBuilder) WithOptions(options ProducerOptions) *ProducerBuilder {
	pb.producerOptions = options
	return pb
}

// Build creates a new Producer instance using the settings configured in the ProducerBuilder.
// It performs validation to ensure that required fields are set before creating the producer.
//
// Returns:
// - *Producer: A pointer to the newly created Producer instance if successful.
// - error: An error if required fields are missing or if producer creation fails.
func (pb *ProducerBuilder) Build() (*Producer, error) {
	if pb.topic == "" {
		return nil, fmt.Errorf("topic must be set")
	}
	if pb.producerName == "" {
		return nil, fmt.Errorf("producer name must be set")
	}

	return newProducer(
		pb.client,
		pb.topic,
		pb.producerName,
		pb.schema,
		pb.producerOptions,
	), nil
}

type ProducerOptions struct {
	// not used yet
	//others string
}
