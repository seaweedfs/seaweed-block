package auth_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/auth"
	"github.com/seaweedfs/seaweed-block/core/auth/authtest"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const costVolumes = 32

// costServer serves the health check behind the given server options and
// interceptor, and returns its address.
func costServer(t *testing.T, opts ...grpc.ServerOption) string {
	t.Helper()
	srv := grpc.NewServer(opts...)
	healthpb.RegisterHealthServer(srv, health.NewServer())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// costRun dials one connection per volume and makes calls unary calls on
// each, all volumes at once (a renewal tick of 32 volumes). It returns every
// volume's first call (the handshake included) and every later call.
func costRun(t *testing.T, addr string, dial func(i int) grpc.DialOption, calls int) (first, rest []time.Duration) {
	t.Helper()
	var mu sync.Mutex
	var wg sync.WaitGroup
	errs := make(chan error, costVolumes)
	for i := 0; i < costVolumes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := link.DialMaster(addr, dial(i))
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			c := healthpb.NewHealthClient(conn)
			var f time.Duration
			var r []time.Duration
			for k := 0; k < calls; k++ {
				start := time.Now()
				if _, err := c.Check(context.Background(), &healthpb.HealthCheckRequest{}); err != nil {
					errs <- fmt.Errorf("volume %d call %d: %w", i, k, err)
					return
				}
				d := time.Since(start)
				if k == 0 {
					f = d
				} else {
					r = append(r, d)
				}
			}
			mu.Lock()
			first, rest = append(first, f), append(rest, r...)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	return first, rest
}

func pct(d []time.Duration, p float64) time.Duration {
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[int(float64(len(s)-1)*p)]
}

// T10: the cost of mutual TLS and the per-call authentication on the renewal
// path, 32 volumes renewing at once, against the same calls in plaintext
// development mode. The bounds are loose (they catch a regression of an order
// of magnitude, not jitter); the measured numbers are logged.
func TestRenewalPathCostAt32Volumes(t *testing.T) {
	if testing.Short() {
		t.Skip("cost measurement")
	}
	dir := t.TempDir()
	ca := authtest.NewCA(t, "ca")
	caPath := ca.WriteCA(t, dir)
	mc, mk := ca.Issue(t, "blockmaster", time.Now().Add(time.Hour)).WriteFiles(t, dir, "master")
	var roles strings.Builder
	nodes := make([]link.TLSFiles, costVolumes)
	for i := range nodes {
		id := fmt.Sprintf("s%02d", i)
		leaf := ca.Issue(t, id, time.Now().Add(time.Hour))
		c, k := leaf.WriteFiles(t, dir, id)
		nodes[i] = link.TLSFiles{Cert: c, Key: k, CA: caPath}
		fmt.Fprintln(&roles, leaf.Fingerprint, "node", id)
	}
	rolePath := filepath.Join(dir, "roles")
	if err := os.WriteFile(rolePath, []byte(roles.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	table, err := auth.LoadRoleFile(rolePath)
	if err != nil {
		t.Fatal(err)
	}
	a := auth.NewAuthenticator(table, nil)
	rules := map[string]auth.Rule{"/grpc.health.v1.Health/Check": {Node: true}}
	creds, err := link.ServerCredentials(link.TLSFiles{Cert: mc, Key: mk, CA: caPath})
	if err != nil {
		t.Fatal(err)
	}
	const calls = 50

	tlsAddr := costServer(t, creds, grpc.UnaryInterceptor(a.UnaryInterceptor(rules)))
	tlsFirst, tlsRest := costRun(t, tlsAddr, func(i int) grpc.DialOption {
		d, err := link.ClientCredentials(nodes[i], "")
		if err != nil {
			t.Fatal(err)
		}
		return d
	}, calls)

	devAddr := costServer(t, grpc.UnaryInterceptor(auth.DevUnaryInterceptor()))
	devFirst, devRest := costRun(t, devAddr, func(int) grpc.DialOption { return nil }, calls)

	t.Logf("T10 %d volumes x %d calls: first call (handshake) p50 %v p99 %v (plaintext %v / %v); per call p50 %v p99 %v (plaintext %v / %v)",
		costVolumes, calls,
		pct(tlsFirst, 0.5), pct(tlsFirst, 0.99), pct(devFirst, 0.5), pct(devFirst, 0.99),
		pct(tlsRest, 0.5), pct(tlsRest, 0.99), pct(devRest, 0.5), pct(devRest, 0.99))
	if p := pct(tlsFirst, 0.99); p > 2*time.Second {
		t.Fatalf("handshake p99 %v at %d volumes: far above the lease renewal budget", p, costVolumes)
	}
	if p := pct(tlsRest, 0.99); p > 100*time.Millisecond {
		t.Fatalf("authenticated per-call p99 %v at %d volumes: far above the lease renewal budget", p, costVolumes)
	}
}
