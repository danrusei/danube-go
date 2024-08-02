# Danube-go client

The Go Client library for interacting with Danube Pub/Sub messaging platform.

[Danube](https://github.com/danrusei/danube) is an open-source **distributed** Pub/Sub messaging platform written in Rust. Consult [the documentation](https://dev-state.com/danube_docs/) for supported concepts and the platform architecture.

I'm working on improving and adding new features. Please feel free to contribute or report any issues you encounter.

## Example usage

Check out the [example files](https://github.com/danrusei/danube-go/tree/main/examples).

### Producer

```go
import (
 "context"
 "encoding/json"
 "fmt"
 "log"

 "github.com/danrusei/danube-go"
)

func main() {

    clientBuilder := &danube.DanubeClientBuilder{}
    client, err := clientBuilder.ServiceURL("127.0.0.1:6650").Build()
        if err != nil {
    log.Fatalf("Failed to create Danube client: %v", err)
    }

    topic := "/default/test_topic"
    jsonSchema := `{"type": "object", "properties": {"field1": {"type": "string"}, "field2": {"type": "integer"}}}`

    ctx := context.Background()
    producer, err := client.NewProducer(ctx)
                     .WithName("test_producer")
                     .WithTopic(topic)
                     .WithSchema("test_schema", danube.SchemaType_JSON, jsonSchema)
                     .Build()
    if err != nil {
     log.Fatalf("unable to initialize the producer: %v", err)
    }

    producerID, err := producer.Create(context.Background())
    if err != nil {
     log.Fatalf("Failed to create producer: %v", err)
    }
    log.Printf("The Producer was created with ID: %v", producerID)

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
}

```

### Consumer

```go

import (
 "context"
 "encoding/json"
 "fmt"
 "log"

 "github.com/danrusei/danube-go"
)

type MyMessage struct {
    Field1 string `json:"field1"`
    Field2 int    `json:"field2"`
}

func main() {

    clientBuilder := &danube.DanubeClientBuilder{}
    client, err := clientBuilder.ServiceURL("127.0.0.1:6650").Build()
        if err != nil {
    log.Fatalf("Failed to create Danube client: %v", err)
    }

    topic := "/default/test_topic"
    subType := danube.Exclusive

    ctx := context.Background()
    consumer, err := client.NewConsumer(ctx)
                    .WithConsumerName("test_consumer")
                    .WithTopic(topic)
                    .WithSubscription("test_subscription")
                    .WithSubscriptionType(subType).Build()
  if err != nil {
    log.Fatalf("Failed to initialize the consumer: %v", err)
    }

    consumerID, err := consumer.Subscribe(ctx)
    if err != nil {
        log.Fatalf("Failed to subscribe: %v", err)
    }
    log.Printf("The Consumer with ID: %v was created", consumerID)

    // Receiving messages
    streamClient, err := consumer.Receive(ctx)
    if err != nil {
        log.Fatalf("Failed to receive messages: %v", err)
    }

    for {
        msg, err := streamClient.Recv()
        if err != nil {
        log.Fatalf("Error receiving message: %v", err)
        break
        }

        var myMessage MyMessage
        if err := json.Unmarshal(msg.GetMessages(), &myMessage); err != nil {
            log.Printf("Failed to decode message: %v", err)
        } else {
            fmt.Printf("Received message: %+v\n", myMessage)
        }
    }
}
```

⚠️ This library is currently under active development and may have missing or incomplete functionalities. Use with caution.
