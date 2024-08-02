package danube

import (
	"context"
	"errors"

	"github.com/danrusei/danube-go/proto" // Path to your generated proto package
)

// SchemaService is used to interact with the schema service
type SchemaService struct {
	CnxManager *ConnectionManager
	RequestID  uint64
}

// NewSchemaService creates a new instance of SchemaService
func NewSchemaService(cnxManager *ConnectionManager) *SchemaService {
	return &SchemaService{
		CnxManager: cnxManager,
		RequestID:  0,
	}
}

// GetSchema retrieves the schema for the given topic
func (ss *SchemaService) GetSchema(ctx context.Context, addr string, topic string) (*Schema, error) {
	conn, err := ss.CnxManager.GetConnection(ctx, addr, addr)
	if err != nil {
		return nil, err
	}

	client := proto.NewDiscoveryClient(conn.grpcConn)

	schemaRequest := &proto.SchemaRequest{
		RequestId: ss.RequestID,
		Topic:     topic,
	}

	response, err := client.GetSchema(ctx, schemaRequest)
	if err != nil {
		return nil, err
	}

	schemaResponse := response.GetSchema()
	if schemaResponse == nil {
		return nil, errors.New("schema response is nil")
	}

	// Convert ProtoSchema to Schema
	schema, err := FromProtoSchema(schemaResponse)
	if err != nil {
		return nil, err
	}

	return schema, nil
}
