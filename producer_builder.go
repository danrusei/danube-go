package danube

import "fmt"

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

func (pb *ProducerBuilder) WithTopic(topic string) *ProducerBuilder {
	pb.topic = topic
	return pb
}

func (pb *ProducerBuilder) WithName(producerName string) *ProducerBuilder {
	pb.producerName = producerName
	return pb
}

func (pb *ProducerBuilder) WithSchema(schemaName string, schemaType SchemaType, schemaData string) *ProducerBuilder {
	pb.schema = NewSchema(schemaName, schemaType, schemaData)
	return pb
}

func (pb *ProducerBuilder) WithOptions(options ProducerOptions) *ProducerBuilder {
	pb.producerOptions = options
	return pb
}

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
