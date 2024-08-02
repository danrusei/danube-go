package danube

import (
	"context"
	"net"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// RpcConnection wraps a gRPC client connection.
type RpcConnection struct {
	grpcConn *grpc.ClientConn
}

// DialOption is a function that configures gRPC dial options.
type DialOption func(*[]grpc.DialOption)

// WithKeepAliveInterval configures the keepalive interval for the connection.
func WithKeepAliveInterval(interval time.Duration) DialOption {
	return func(opts *[]grpc.DialOption) {
		*opts = append(*opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: interval,
		}))
	}
}

// WithConnectionTimeout configures the connection timeout for the connection.
func WithConnectionTimeout(timeout time.Duration) DialOption {
	return func(opts *[]grpc.DialOption) {
		*opts = append(*opts, grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			dialer := &net.Dialer{Timeout: timeout}
			return dialer.DialContext(ctx, "tcp", addr)
		}))
	}
}

// NewRpcConnection creates a new RpcConnection with the given options.
func NewRpcConnection(connectURL string, options ...DialOption) (*RpcConnection, error) {
	var dialOptions []grpc.DialOption

	// Apply default options
	dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// Apply additional options
	for _, opt := range options {
		opt(&dialOptions)
	}

	conn, err := grpc.NewClient(connectURL, dialOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect")
	}
	return &RpcConnection{grpcConn: conn}, nil
}
