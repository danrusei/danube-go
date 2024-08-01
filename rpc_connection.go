package danube

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

type RpcConnection struct {
	grpcConn *grpc.ClientConn
}

func newRpcConnection(options *ConnectionOptions, connectURL string) (*RpcConnection, error) {
	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), // For development only. Use secure credentials in production.
	}

	if options.KeepAliveInterval > 0 {
		dialOptions = append(dialOptions, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: options.KeepAliveInterval,
		}))
	}

	if options.ConnectionTimeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), options.ConnectionTimeout)
		defer cancel()
		conn, err := grpc.DialContext(ctx, connectURL, dialOptions...)
		if err != nil {
			return nil, errors.Wrap(err, "failed to connect")
		}
		return &RpcConnection{grpcConn: conn}, nil
	}

	conn, err := grpc.NewClient(connectURL, dialOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect")
	}
	return &RpcConnection{grpcConn: conn}, nil
}
