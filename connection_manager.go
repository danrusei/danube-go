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
	connections        map[BrokerAddress]*ConnectionStatus
	connection_options []DialOption
	connectionsMutex   sync.Mutex
}

// NewConnectionManager creates a new ConnectionManager.
func NewConnectionManager(options []DialOption) *ConnectionManager {
	return &ConnectionManager{
		connections:        make(map[BrokerAddress]*ConnectionStatus),
		connection_options: options,
	}
}

func (cm *ConnectionManager) GetConnection(ctx context.Context, brokerURL, connectURL string, options ...DialOption) (*RpcConnection, error) {
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

	rpcConn, err := NewRpcConnection(connectURL, options...)
	if err != nil {
		return nil, err
	}

	if !exists {
		cm.connections[broker] = &ConnectionStatus{}
	}
	cm.connections[broker].Connected = rpcConn
	return rpcConn, nil
}
