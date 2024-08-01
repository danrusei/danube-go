package danube

import (
	"context"
	"sync"
)

type BrokerAddress struct {
	ConnectURL string
	BrokerURL  string
	Proxy      bool
}

type ConnectionStatus struct {
	Connected    *RpcConnection
	Disconnected bool
}

type ConnectionManager struct {
	connections       map[BrokerAddress]*ConnectionStatus
	connectionsMutex  sync.Mutex
	ConnectionOptions ConnectionOptions
}

func NewConnectionManager(options ConnectionOptions) *ConnectionManager {
	return &ConnectionManager{
		connections:       make(map[BrokerAddress]*ConnectionStatus),
		ConnectionOptions: options,
	}
}

func (cm *ConnectionManager) GetConnection(ctx context.Context, brokerURL, connectURL string) (*RpcConnection, error) {
	cm.connectionsMutex.Lock()
	defer cm.connectionsMutex.Unlock()

	proxy := brokerURL == connectURL
	broker := BrokerAddress{
		ConnectURL: connectURL,
		BrokerURL:  brokerURL,
		Proxy:      proxy,
	}

	status, exists := cm.connections[broker]
	if exists && status.Connected != nil {
		return status.Connected, nil
	}

	rpcConn, err := newRpcConnection(&cm.ConnectionOptions, connectURL)
	if err != nil {
		return nil, err
	}

	if !exists {
		cm.connections[broker] = &ConnectionStatus{}
	}
	cm.connections[broker].Connected = rpcConn
	return rpcConn, nil
}
