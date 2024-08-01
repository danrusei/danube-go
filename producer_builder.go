package danube

import "fmt"

type ProducerBuilder struct {
	client          *DanubeClient
	topic           *string
	producerName    *string
	schema          *Schema
	producerOptions ProducerOptions
}

func NewProducerBuilder(client *DanubeClient) *ProducerBuilder {
	return &ProducerBuilder{
		client:          client,
		topic:           nil,
		producerName:    nil,
		schema:          nil,
		producerOptions: ProducerOptions{},
	}
}

func (pb *ProducerBuilder) WithTopic(topic string) *ProducerBuilder {
	pb.topic = &topic
	return pb
}

func (pb *ProducerBuilder) WithName(producerName string) *ProducerBuilder {
	pb.producerName = &producerName
	return pb
}

func (pb *ProducerBuilder) WithSchema(schemaName string, schemaType SchemaType) *ProducerBuilder {
	pb.schema = &Schema{Name: schemaName, Type: schemaType}
	return pb
}

func (pb *ProducerBuilder) WithOptions(options ProducerOptions) *ProducerBuilder {
	pb.producerOptions = options
	return pb
}

func (pb *ProducerBuilder) Build() (*Producer, error) {
	if pb.topic == nil {
		return nil, fmt.Errorf("topic must be set")
	}
	if pb.producerName == nil {
		return nil, fmt.Errorf("producer name must be set")
	}

	return NewProducer(
		pb.client,
		*pb.topic,
		*pb.producerName,
		pb.schema,
		pb.producerOptions,
	), nil
}
