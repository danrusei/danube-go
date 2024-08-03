package danube

import (
	"sync"
)

type BrokerAddress struct {
	ConnectURL string
	BrokerURL  string
	Proxy      bool
}

type connectionStatus struct {
	Connected    *rpcConnection
	Disconnected bool
}

type connectionManager struct {
	connections        map[BrokerAddress]*connectionStatus
	connection_options []DialOption
	connectionsMutex   sync.Mutex
}

// NewConnectionManager creates a new ConnectionManager.
func newConnectionManager(options []DialOption) *connectionManager {
	return &connectionManager{
		connections:        make(map[BrokerAddress]*connectionStatus),
		connection_options: options,
	}
}

func (cm *connectionManager) getConnection(brokerURL, connectURL string, options ...DialOption) (*rpcConnection, error) {
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

	rpcConn, err := newRpcConnection(connectURL, options...)
	if err != nil {
		return nil, err
	}

	if !exists {
		cm.connections[broker] = &connectionStatus{}
	}
	cm.connections[broker].Connected = rpcConn
	return rpcConn, nil
}
