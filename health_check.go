package danube

import (
	"context"
	"log"
	"time"

	"github.com/danrusei/danube-go/proto" // Path to your generated proto package
)

type HealthCheckService struct {
	CnxManager *ConnectionManager
	RequestID  uint64
}

func NewHealthCheckService(cnxManager *ConnectionManager) *HealthCheckService {
	return &HealthCheckService{
		CnxManager: cnxManager,
		RequestID:  0,
	}
}

func (hcs *HealthCheckService) StartHealthCheck(
	ctx context.Context,
	addr string,
	clientType int32,
	clientID uint64,
	stopSignal *StopSignal,
) error {
	conn, err := hcs.CnxManager.GetConnection(ctx, addr)
	if err != nil {
		return err
	}

	client := proto.NewHealthCheckClient(conn)
	go func() {
		for {
			if stopSignal.IsStopped() {
				return
			}
			err := healthCheck(ctx, client, hcs.RequestID, clientType, clientID, stopSignal)
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
	stopSignal *StopSignal,
) error {
	healthRequest := &proto.HealthCheckRequest{
		RequestId: requestID,
		Client:    clientType,
		Id:        clientID,
	}

	response, err := client.HealthCheck(ctx, healthRequest)
	if err != nil {
		return err
	}

	if response.GetStatus() == proto.ClientStatus_CLOSE {
		log.Printf("Received stop signal from broker in health check response")
		stopSignal.SetStopped(true)
		return nil
	}
	return nil
}

type StopSignal struct {
	stopped chan struct{}
}

func NewStopSignal() *StopSignal {
	return &StopSignal{
		stopped: make(chan struct{}, 1),
	}
}

func (ss *StopSignal) IsStopped() bool {
	select {
	case <-ss.stopped:
		return true
	default:
		return false
	}
}

func (ss *StopSignal) SetStopped(stopped bool) {
	if stopped {
		select {
		case ss.stopped <- struct{}{}:
		default:
		}
	}
}
