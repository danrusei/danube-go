package danube

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/danrusei/danube-go/proto" // Path to your generated proto package
)

type HealthCheckService struct {
	CnxManager *ConnectionManager
	RequestID  atomic.Uint64
}

func NewHealthCheckService(cnxManager *ConnectionManager) *HealthCheckService {
	return &HealthCheckService{
		CnxManager: cnxManager,
		RequestID:  atomic.Uint64{},
	}
}

func (hcs *HealthCheckService) StartHealthCheck(
	ctx context.Context,
	addr string,
	clientType int32,
	clientID uint64,
	stopSignal *atomic.Bool,
) error {
	conn, err := hcs.CnxManager.GetConnection(ctx, addr, addr)
	if err != nil {
		return err
	}

	client := proto.NewHealthCheckClient(conn.grpcConn)
	go func() {
		for {
			err := healthCheck(ctx, client, hcs.RequestID.Add(1), clientType, clientID, stopSignal)
			if err != nil {
				log.Printf("Error in health check: %v", err)
				return
			}
			time.Sleep(5 * time.Second)
		}
	}()
	return nil
}

func healthCheck(
	ctx context.Context,
	client proto.HealthCheckClient,
	requestID uint64,
	clientType int32,
	clientID uint64,
	stopSignal *atomic.Bool,
) error {
	healthRequest := &proto.HealthCheckRequest{
		RequestId: requestID,
		Client:    proto.HealthCheckRequest_ClientType(clientType),
		Id:        clientID,
	}

	response, err := client.HealthCheck(ctx, healthRequest)
	if err != nil {
		return err
	}

	if response.GetStatus() == proto.HealthCheckResponse_CLOSE {
		log.Printf("Received stop signal from broker in health check response")
		stopSignal.Store(true)
		return nil
	}
	return nil
}
