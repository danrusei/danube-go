package danube

import (
	"fmt"
	// Adjust the import path for your generated protobuf code
)

type SchemaType int32

const (
	SchemaType_BYTES  SchemaType = 0
	SchemaType_STRING SchemaType = 1
	SchemaType_INT64  SchemaType = 2
	SchemaType_JSON   SchemaType = 3
)

type Schema struct {
	Name       string
	SchemaData []byte
	TypeSchema SchemaType
}

func NewSchema(name string, schemaType SchemaType, jsonSchema string) *Schema {
	schemaData := []byte{}
	if schemaType == SchemaType_JSON {
		schemaData = []byte(jsonSchema)
	}
	return &Schema{
		Name:       name,
		SchemaData: schemaData,
		TypeSchema: schemaType,
	}
}

// Convert SchemaType to Protobuf representation
func (s SchemaType) ToProto() proto.schema_pb.TypeSchema {
	switch s {
	case SchemaType_BYTES:
		return schema_pb.TypeSchema_BYTES
	case SchemaType_STRING:
		return schema_pb.TypeSchema_STRING
	case SchemaType_INT64:
		return schema_pb.TypeSchema_INT64
	case SchemaType_JSON:
		return schema_pb.TypeSchema_JSON
	default:
		return schema_pb.TypeSchema_UNKNOWN
	}
}

// Convert Protobuf TypeSchema to SchemaType
func FromProtoTypeSchema(protoSchema schema_pb.TypeSchema) SchemaType {
	switch protoSchema {
	case schema_pb.TypeSchema_BYTES:
		return SchemaType_BYTES
	case schema_pb.TypeSchema_STRING:
		return SchemaType_STRING
	case schema_pb.TypeSchema_INT64:
		return SchemaType_INT64
	case schema_pb.TypeSchema_JSON:
		return SchemaType_JSON
	default:
		return SchemaType_JSON // Default to JSON if unknown
	}
}

// Convert Protobuf Schema to Schema
func FromProtoSchema(protoSchema *schema_pb.Schema) (*Schema, error) {
	typeSchema := FromProtoTypeSchema(protoSchema.GetTypeSchema())
	return &Schema{
		Name:       protoSchema.GetName(),
		SchemaData: protoSchema.GetSchemaData(),
		TypeSchema: typeSchema,
	}, nil
}

// Convert Schema to Protobuf Schema
func (s *Schema) ToProto() *schema_pb.Schema {
	return &schema_pb.Schema{
		Name:       s.Name,
		SchemaData: s.SchemaData,
		TypeSchema: s.TypeSchema.ToProto(),
	}
}

// Convert JSON Schema to a Go string
func (s *Schema) JSONSchema() (string, error) {
	if s.TypeSchema != SchemaType_JSON {
		return "", fmt.Errorf("schema type is not JSON")
	}
	return string(s.SchemaData), nil
}

// Convert Protobuf Schema to JSON
func ProtoSchemaToJSON(protoSchema *schema_pb.Schema) (string, error) {
	schema, err := FromProtoSchema(protoSchema)
	if err != nil {
		return "", err
	}
	return schema.JSONSchema()
}
