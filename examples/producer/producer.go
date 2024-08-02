package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/danrusei/danube-go"
)

func main() {
	// Setup logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	clientBuilder := &danube.DanubeClientBuilder{}
	clientBuilder.ServiceURL("127.0.0.1:6650")
	client, err := clientBuilder.Build()
	if err != nil {
		log.Fatalf("Failed to create Danube client: %v", err)
	}

	topic := "/default/test_topic"
	jsonSchema := `{"type": "object", "properties": {"field1": {"type": "string"}, "field2": {"type": "integer"}}}`

	ctx := context.Background()
	producer, err := client.NewProducer(ctx).WithName("test_producer").WithTopic(topic).WithSchema("test_schema", danube.SchemaType_JSON, jsonSchema).Build()
	if err != nil {
		log.Fatalf("unable to initialize the producer: %v", err)
	}

	producerID, err := producer.Create(context.Background())
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	log.Printf("The Producer was created with ID: %v", producerID)

	for i := 0; i < 20; i++ {
		data := map[string]interface{}{
			"field1": fmt.Sprintf("value%d", i),
			"field2": 2020 + i,
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			log.Fatalf("Failed to marshal data: %v", err)
		}

		messageID, err := producer.Send(context.Background(), jsonData)
		if err != nil {
			log.Fatalf("Failed to send message: %v", err)
		}
		log.Printf("The Message with id %v was sent", messageID)

		time.Sleep(1 * time.Second)
	}
}
