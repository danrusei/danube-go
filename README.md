# Danube-go client

The Go Client library for interacting with Danube Pub/Sub messaging platform.

[Danube](https://github.com/danrusei/danube) is an open-source **distributed** Pub/Sub messaging platform written in Rust. Consult [the documentation](https://dev-state.com/danube_docs/) for supported concepts and the platform architecture.

I'm working on improving and adding new features. Please feel free to contribute or report any issues you encounter.

## Example usage

Check out the [example files](https://github.com/danrusei/danube-go/tree/main/examples).

### Start the Danube server

Use the [instructions from the documentation](https://dev-state.com/danube_docs/) to run the Danube broker/cluster.

### Create Producer

```go
client := danube.NewClient().ServiceURL("127.0.0.1:6650").Build()

ctx := context.Background()
topic := "/default/test_topic"

// create the client Producer instance
producer, err := client.NewProducer(ctx).
    WithName("test_producer").
    WithTopic(topic).
    Build()
if err != nil {
    log.Fatalf("unable to initialize the producer: %v", err)
    }

// create the Producer instance on the Danube cluster
producerID, err := producer.Create(ctx)
if err != nil {
    log.Fatalf("Failed to create producer: %v", err)
}
log.Printf("The Producer was created with ID: %v", producerID)

payload := fmt.Sprintf("Hello Danube %d", i)
bytes_payload := []byte(payload)

// send the message
messageID, err := producer.Send(ctx, bytes_payload, nil)
if err != nil {
 log.Fatalf("Failed to send message: %v", err)
}
log.Printf("The Message with id %v was sent", messageID)

```

### Create Consumer

```go
client := danube.NewClient().ServiceURL("127.0.0.1:6650").Build()

ctx := context.Background()
topic := "/default/test_topic"
// choose Exclusive subscription, but could be Shared as well
subType := danube.Exclusive

// create the client Consumer instance
consumer, err := client.NewConsumer(ctx).
    WithConsumerName("test_consumer").
    WithTopic(topic).
    WithSubscription("test_subscription").
    WithSubscriptionType(subType).
    Build()
if err != nil {
    log.Fatalf("Failed to initialize the consumer: %v", err)
    }

// subscribe to the newly created subscription
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

fmt.Printf("Received message: %+v\n", string(msg.GetPayload()))
}
```

⚠️ This library is currently under active development and may have missing or incomplete functionalities. Use with caution.

## Contribution

### Use latest DanubeApi.proto file

Make sure the proto/DanubeApi.proto is the latest from [Danube project](https://github.com/danrusei/danube/tree/main/proto).

If not replace the file and add at the top of the file

```bash
option go_package = "github.com/danrusei/danube-go/proto";
```

right after the `package danube;`

In order to generate the Go code you need the following packages installed:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

And generate the Go code from the proto file:

```bash
protoc --proto_path=./proto --go_out=./proto --go-grpc_out=./proto --go_opt=paths=source_relative      --go-grpc_opt=paths=source_relative proto/DanubeApi.proto
```
