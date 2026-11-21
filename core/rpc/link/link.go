// Package link builds every gRPC link between seaweed-block processes and to
// SeaweedFS: one dial helper and one server builder, so transport security
// and call authorization have a single place to be added. Today a link is
// plaintext, exactly as before this package existed, and a server runs only
// the interceptors its caller passes (none by default).
package link

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Dial opens a lazy client connection to target with plaintext transport
// credentials; opts follow (a later option overrides an earlier one).
func Dial(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	all := make([]grpc.DialOption, 0, len(opts)+1)
	all = append(all, grpc.WithTransportCredentials(insecure.NewCredentials()))
	all = append(all, opts...)
	return grpc.NewClient(target, all...)
}

// ServerConfig is a server's interceptor chain; the zero value has none.
type ServerConfig struct {
	Unary  []grpc.UnaryServerInterceptor
	Stream []grpc.StreamServerInterceptor
}

// NewServer builds a gRPC server whose calls pass through cfg's interceptors,
// in order; opts follow.
func NewServer(cfg ServerConfig, opts ...grpc.ServerOption) *grpc.Server {
	all := make([]grpc.ServerOption, 0, len(opts)+2)
	if len(cfg.Unary) > 0 {
		all = append(all, grpc.ChainUnaryInterceptor(cfg.Unary...))
	}
	if len(cfg.Stream) > 0 {
		all = append(all, grpc.ChainStreamInterceptor(cfg.Stream...))
	}
	all = append(all, opts...)
	return grpc.NewServer(all...)
}
