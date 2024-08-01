package danube

import (
	"github.com/danrusei/danube-go/proto" // Path to your generated proto package
)

// MessageMetadata holds metadata about a message
type MessageMetadata struct {
	ProducerName string
	SequenceId   uint64
	PublishTime  uint64
}

// SendMessage contains information needed to send a message
type SendMessage struct {
	RequestId  uint64
	ProducerId uint64
	Metadata   *MessageMetadata
	Message    []byte
}

// ToProto converts SendMessage to the Protobuf MessageRequest
func (sm *SendMessage) ToProto() *proto.MessageRequest {
	var metadata *proto.MessageMetadata
	if sm.Metadata != nil {
		metadata = &proto.MessageMetadata{
			ProducerName: sm.Metadata.ProducerName,
			SequenceId:   sm.Metadata.SequenceId,
			PublishTime:  sm.Metadata.PublishTime,
		}
	}

	return &proto.MessageRequest{
		RequestId:  sm.RequestId,
		ProducerId: sm.ProducerId,
		Metadata:   metadata,
		Message:    sm.Message,
	}
}
