package danube

import (
	"errors"
	// Path to your generated proto package
)

type ConsumerBuilder struct {
	client           *DanubeClient
	topic            string
	consumerName     string
	subscription     string
	subscriptionType *SubType
	consumerOptions  ConsumerOptions
}

func newConsumerBuilder(client *DanubeClient) *ConsumerBuilder {
	return &ConsumerBuilder{client: client}
}

func (b *ConsumerBuilder) WithTopic(topic string) *ConsumerBuilder {
	b.topic = topic
	return b
}

func (b *ConsumerBuilder) WithConsumerName(name string) *ConsumerBuilder {
	b.consumerName = name
	return b
}

func (b *ConsumerBuilder) WithSubscription(subscription string) *ConsumerBuilder {
	b.subscription = subscription
	return b
}

func (b *ConsumerBuilder) WithSubscriptionType(subType SubType) *ConsumerBuilder {
	b.subscriptionType = &subType
	return b
}

func (b *ConsumerBuilder) Build() (*Consumer, error) {
	if b.topic == "" || b.consumerName == "" || b.subscription == "" {
		return nil, errors.New("missing required fields")
	}
	return newConsumer(b.client, b.topic, b.consumerName, b.subscription, b.subscriptionType, b.consumerOptions), nil
}

type ConsumerOptions struct {
	Others string
}
