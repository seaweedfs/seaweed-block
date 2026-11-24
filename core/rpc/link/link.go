// Package link builds every gRPC link between seaweed-block processes and to
// SeaweedFS: one dial helper and one server builder, so transport security
// and call authorization have a single place to be added. Today a link is
// plaintext, exactly as before this package existed, and a server runs only
// the interceptors its caller passes (none by default).
package link

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
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

// DialWith opens a lazy client connection to target with exactly opts: the
// caller supplies the transport credentials (no default is added). It is the
// one place a link with caller-chosen options is built.
func DialWith(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.NewClient(target, opts...)
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

// MasterReconnect caps the reconnect back-off of a long-lived link to
// blockmaster. A volume process waits for an unreachable master instead of
// exiting, so one channel can live through a long outage; with gRPC's default
// back-off (base 1 s, x1.6, jitter 0.2, max 120 s: grpc@v1.80.0
// backoff/backoff.go:48-51) the first call after the master returns could lag
// by up to two minutes. Capped at 2 s, the lag stays about 2 s. The connect
// timeout must be set: WithConnectParams otherwise makes it equal to the
// back-off step (dialoptions.go:313-314), too short for a TLS handshake.
func MasterReconnect() grpc.DialOption {
	return grpc.WithConnectParams(grpc.ConnectParams{
		Backoff:           backoff.Config{BaseDelay: time.Second, Multiplier: 1.6, Jitter: 0.2, MaxDelay: 2 * time.Second},
		MinConnectTimeout: 5 * time.Second,
	})
}
