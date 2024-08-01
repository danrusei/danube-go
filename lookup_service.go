package danube

import (
	"context"
	"errors"

	"github.com/danrusei/danube-go/proto" // Path to your generated proto package
)

// LookupResult holds the result of a topic lookup
type LookupResult struct {
	ResponseType int32
	Addr         string
}

// LookupService handles lookup operations
type LookupService struct {
	CnxManager *ConnectionManager
	RequestID  uint64
}

// NewLookupService creates a new instance of LookupService
func NewLookupService(cnxManager *ConnectionManager) *LookupService {
	return &LookupService{
		CnxManager: cnxManager,
		RequestID:  0,
	}
}

// LookupTopic performs the topic lookup request
func (ls *LookupService) LookupTopic(ctx context.Context, addr string, topic string) (*LookupResult, error) {
	conn, err := ls.CnxManager.GetConnection(ctx, addr)
	if err != nil {
		return nil, err
	}

	client := proto.NewDiscoveryClient(conn)

	lookupRequest := &proto.TopicLookupRequest{
		RequestId: ls.RequestID,
		Topic:     topic,
	}

	response, err := client.TopicLookup(ctx, lookupRequest)
	if err != nil {
		return nil, err
	}

	lookupResp := response.GetLookupResponse()
	addrToUri := lookupResp.GetBrokerServiceUrl()

	return &LookupResult{
		ResponseType: lookupResp.GetResponseType(),
		Addr:         addrToUri,
	}, nil
}

// HandleLookup processes the lookup request and returns the appropriate URI
func (ls *LookupService) HandleLookup(ctx context.Context, addr string, topic string) (string, error) {
	lookupResult, err := ls.LookupTopic(ctx, addr, topic)
	if err != nil {
		return "", err
	}

	switch lookupResult.ResponseType {
	case int32(proto.LookupType_REDIRECT):
		return lookupResult.Addr, nil
	case int32(proto.LookupType_CONNECT):
		return addr, nil
	case int32(proto.LookupType_FAILED):
		return "", errors.New("lookup failed")
	default:
		return "", errors.New("unknown lookup type")
	}
}

// LookupTypeFromInt converts an integer to a LookupType
func LookupTypeFromInt(value int32) (proto.LookupType, error) {
	switch value {
	case 0:
		return proto.LookupType_REDIRECT, nil
	case 1:
		return proto.LookupType_CONNECT, nil
	case 2:
		return proto.LookupType_FAILED, nil
	default:
		return proto.LookupType_UNKNOWN, errors.New("unknown lookup type")
	}
}
