package danube

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/danrusei/danube-go/proto" // Path to your generated proto package
)

// LookupResult holds the result of a topic lookup
type LookupResult struct {
	ResponseType proto.TopicLookupResponse_LookupType
	Addr         string
}

// LookupService handles lookup operations
type LookupService struct {
	cnxManager *connectionManager
	RequestID  atomic.Uint64
}

// NewLookupService creates a new instance of LookupService
func NewLookupService(cnxManager *connectionManager) *LookupService {
	return &LookupService{
		cnxManager: cnxManager,
		RequestID:  atomic.Uint64{},
	}
}

// LookupTopic performs the topic lookup request
func (ls *LookupService) LookupTopic(ctx context.Context, addr string, topic string) (*LookupResult, error) {
	conn, err := ls.cnxManager.getConnection(addr, addr)
	if err != nil {
		return nil, err
	}

	client := proto.NewDiscoveryClient(conn.grpcConn)

	lookupRequest := &proto.TopicLookupRequest{
		RequestId: ls.RequestID.Add(1),
		Topic:     topic,
	}

	response, err := client.TopicLookup(ctx, lookupRequest)
	if err != nil {
		return nil, err
	}

	return &LookupResult{
		ResponseType: response.GetResponseType(),
		Addr:         response.GetBrokerServiceUrl(),
	}, nil
}

// HandleLookup processes the lookup request and returns the appropriate URI
func (ls *LookupService) HandleLookup(ctx context.Context, addr string, topic string) (string, error) {
	lookupResult, err := ls.LookupTopic(ctx, addr, topic)
	if err != nil {
		return "", err
	}

	switch lookupResult.ResponseType {
	case proto.TopicLookupResponse_Redirect:
		return lookupResult.Addr, nil
	case proto.TopicLookupResponse_Connect:
		return addr, nil
	case proto.TopicLookupResponse_Failed:
		return "", errors.New("lookup failed")
	default:
		return "", errors.New("unknown lookup type")
	}
}
