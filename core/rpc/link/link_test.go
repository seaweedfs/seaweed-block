package link

import (
	"context"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func serve(t *testing.T, cfg ServerConfig) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(cfg)
	healthpb.RegisterHealthServer(srv, health.NewServer())
	done := make(chan struct{})
	go func() { _ = srv.Serve(lis); close(done) }()
	t.Cleanup(func() {
		srv.Stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
	})
	return lis.Addr().String()
}

// A link dialed with Dial reaches a server built with NewServer, for a unary
// and a streaming call, as plaintext gRPC did before the seam.
func TestLink_DialReachesNewServerUnaryAndStream(t *testing.T) {
	addr := serve(t, ServerConfig{})
	conn, err := Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c := healthpb.NewHealthClient(conn)
	if r, err := c.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil || r.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("unary call: %v %v", r, err)
	}
	w, err := c.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if r, err := w.Recv(); err != nil || r.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("stream call: %v %v", r, err)
	}
}

// The server runs its unary and stream interceptors in the configured order.
func TestLink_ServerRunsInterceptorsInOrder(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	note := func(s string) { mu.Lock(); seen = append(seen, s); mu.Unlock() }
	unary := func(name string) grpc.UnaryServerInterceptor {
		return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			note(name)
			return h(ctx, req)
		}
	}
	stream := func(name string) grpc.StreamServerInterceptor {
		return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			note(name)
			return h(srv, ss)
		}
	}
	addr := serve(t, ServerConfig{Unary: []grpc.UnaryServerInterceptor{unary("u1"), unary("u2")}, Stream: []grpc.StreamServerInterceptor{stream("s1"), stream("s2")}})
	conn, err := Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c := healthpb.NewHealthClient(conn)
	if _, err := c.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	w, err := c.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Recv(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if got := strings.Join(seen, ","); got != "u1,u2,s1,s2" {
		t.Fatalf("interceptors ran as %q, want u1,u2,s1,s2", got)
	}
}

// Every production gRPC link goes through this package: no other non-test Go
// file builds plaintext credentials or a gRPC server directly.
func TestLink_NoProductionSiteBypassesTheSeam(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	self, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	var bad []string
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); name == ".git" || name == "vendor" || name == "testdata" || p == self {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		src := string(b)
		if strings.Contains(src, "insecure.NewCredentials(") || strings.Contains(src, "grpc.NewServer(") {
			rel, _ := filepath.Rel(root, p)
			bad = append(bad, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(bad) > 0 {
		t.Fatalf("gRPC links built outside core/rpc/link (use link.Dial / link.NewServer): %v", bad)
	}
}
