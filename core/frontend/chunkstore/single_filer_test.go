package chunkstore

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/masterpb"
)

type configFiler struct {
	filerpb.UnimplementedSeaweedFilerServer
	mu      sync.Mutex
	masters []string
	group   string
	err     error
}

func (f *configFiler) GetFilerConfiguration(context.Context, *filerpb.GetFilerConfigurationRequest) (*filerpb.GetFilerConfigurationResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return &filerpb.GetFilerConfigurationResponse{Masters: f.masters, FilerGroup: f.group}, nil
}

type nodesMaster struct {
	masterpb.UnimplementedSeaweedServer
	mu     sync.Mutex
	filers []string
	emptyN int  // the first emptyN calls list no filer (not registered yet)
	stall  bool // never answers (until the caller's deadline)
	calls  int
	got    *masterpb.ListClusterNodesRequest
}

func (m *nodesMaster) ListClusterNodes(ctx context.Context, r *masterpb.ListClusterNodesRequest) (*masterpb.ListClusterNodesResponse, error) {
	m.mu.Lock()
	m.got = r
	m.calls++
	calls, stall := m.calls, m.stall
	filers := append([]string(nil), m.filers...)
	m.mu.Unlock()
	if stall {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	resp := &masterpb.ListClusterNodesResponse{}
	if calls <= m.emptyN {
		return resp, nil
	}
	for _, a := range filers {
		resp.ClusterNodes = append(resp.ClusterNodes, &masterpb.ListClusterNodesResponse_ClusterNode{Address: a})
	}
	return resp, nil
}

func (m *nodesMaster) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// startMaster serves a fake master and returns its address in SeaweedFS's
// "host:httpPort.grpcPort" form.
func startMaster(t *testing.T, m *nodesMaster) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	masterpb.RegisterSeaweedServer(srv, m)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	_, port, _ := net.SplitHostPort(lis.Addr().String())
	return "127.0.0.1:9333." + port
}

// deadAddress is a gRPC port nothing listens on.
func deadAddress(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, _ := net.SplitHostPort(lis.Addr().String())
	_ = lis.Close()
	return "127.0.0.1:9333." + port
}

// startConfigFiler serves a fake filer and dials it. It returns the client
// and the filer's address as a master lists it ("host:httpPort.grpcPort").
func startConfigFiler(t *testing.T, f *configFiler) (*SWFS, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	filerpb.RegisterSeaweedFilerServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	c, err := DialSWFS(SWFSConfig{FilerGRPC: lis.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_, port, _ := net.SplitHostPort(lis.Addr().String())
	return c, "127.0.0.1:8888." + port
}

// cluster starts a fake filer and one fake master per entry of masters; each
// master's listing is built from the filer's own listed address (self).
func cluster(t *testing.T, group string, masters ...func(self string) *nodesMaster) (*SWFS, []*nodesMaster) {
	t.Helper()
	f := &configFiler{group: group}
	c, self := startConfigFiler(t, f)
	var ms []*nodesMaster
	var addrs []string
	for _, mk := range masters {
		m := mk(self)
		ms = append(ms, m)
		addrs = append(addrs, startMaster(t, m))
	}
	f.mu.Lock()
	f.masters = addrs
	f.mu.Unlock()
	return c, ms
}

func lists(filers ...string) func(string) *nodesMaster {
	return func(self string) *nodesMaster {
		var out []string
		for _, a := range filers {
			if a == "self" {
				a = self
			}
			out = append(out, a)
		}
		return &nodesMaster{filers: out}
	}
}

func shortBounds(t *testing.T) {
	t.Helper()
	w, r, a := singleFilerWait, singleFilerRetry, singleFilerAttempt
	singleFilerWait, singleFilerRetry, singleFilerAttempt = 1500*time.Millisecond, 100*time.Millisecond, 300*time.Millisecond
	t.Cleanup(func() { singleFilerWait, singleFilerRetry, singleFilerAttempt = w, r, a })
}

func refusedAtOnce(t *testing.T, what string, c *SWFS, want string) {
	t.Helper()
	start := time.Now()
	err := c.CheckSingleFiler(context.Background())
	if !errors.Is(err, ErrNotSingleFiler) || !strings.Contains(err.Error(), want) || time.Since(start) > 700*time.Millisecond {
		t.Fatalf("%s: %v after %s; want ErrNotSingleFiler naming %q, at once", what, err, time.Since(start), want)
	}
}

func refusedAtBound(t *testing.T, what string, c *SWFS) {
	t.Helper()
	start := time.Now()
	err := c.CheckSingleFiler(context.Background())
	if !errors.Is(err, ErrNotSingleFiler) || time.Since(start) < singleFilerWait {
		t.Fatalf("%s: %v after %s; want a refusal only at the %s bound", what, err, time.Since(start), singleFilerWait)
	}
}

// The configured filer passes only as the one filer of its group, listed by
// a master; also when it registers only after a few rounds.
func TestCheckSingleFilerPasses(t *testing.T) {
	shortBounds(t)
	c, ms := cluster(t, "g1", lists("self"))
	if err := c.CheckSingleFiler(context.Background()); err != nil {
		t.Fatalf("the target as the one filer: %v", err)
	}
	if ms[0].got.GetClientType() != "filer" || ms[0].got.GetFilerGroup() != "g1" {
		t.Fatalf("master asked for %q in group %q; want filer in g1", ms[0].got.GetClientType(), ms[0].got.GetFilerGroup())
	}
	c, ms = cluster(t, "", func(self string) *nodesMaster { return &nodesMaster{filers: []string{self}, emptyN: 3} })
	if err := c.CheckSingleFiler(context.Background()); err != nil || ms[0].callCount() != 4 {
		t.Fatalf("target registered on the 4th round: %v after %d calls; want a pass", err, ms[0].callCount())
	}
	// A dead or stalled first master does not hide a healthy second one, and
	// a stalled call costs one attempt, not the whole bound.
	c, _ = cluster(t, "", func(string) *nodesMaster { return &nodesMaster{stall: true} }, lists("self"))
	start := time.Now()
	if err := c.CheckSingleFiler(context.Background()); err != nil || time.Since(start) > 2*singleFilerAttempt {
		t.Fatalf("stalled first master: %v after %s; want a pass within one attempt", err, time.Since(start))
	}
	f := &configFiler{}
	c, self := startConfigFiler(t, f)
	f.mu.Lock()
	f.masters = []string{deadAddress(t), startMaster(t, &nodesMaster{filers: []string{self}})}
	f.mu.Unlock()
	if err := c.CheckSingleFiler(context.Background()); err != nil {
		t.Fatalf("dead first master: %v", err)
	}
}

// Refusals, in both master orders: several filers, or another filer listed
// as the sole filer while the target is not registered, refuse at once,
// whichever master answers first.
func TestCheckSingleFilerRefusesAnotherOrSeveralFilers(t *testing.T) {
	shortBounds(t)
	c, _ := cluster(t, "", lists("self", "10.0.0.2:8888"))
	refusedAtOnce(t, "two filers", c, "10.0.0.2:8888")
	c, _ = cluster(t, "", lists("10.9.9.9:8888"))
	refusedAtOnce(t, "another sole filer, target not registered", c, "10.9.9.9:8888")
	c, _ = cluster(t, "", lists("self"), lists("self", "10.0.0.2:8888"))
	refusedAtOnce(t, "first master favourable, second lists two", c, "10.0.0.2:8888")
	c, _ = cluster(t, "", lists("self", "10.0.0.2:8888"), lists("self"))
	refusedAtOnce(t, "first master lists two, second favourable", c, "10.0.0.2:8888")
	c, _ = cluster(t, "", lists("self"), lists("10.9.9.9:8888"))
	refusedAtOnce(t, "masters disagree on the sole filer", c, "10.9.9.9:8888")
	c, _ = cluster(t, "", lists("10.9.9.9:8888"), lists("self"))
	refusedAtOnce(t, "masters disagree on the sole filer, reversed", c, "10.9.9.9:8888")
}

// Fail closed at the bound, after retrying: no filer listed, an unreachable
// master; and at once: a filer without configuration or without a master.
func TestCheckSingleFilerFailsClosed(t *testing.T) {
	shortBounds(t)
	c, ms := cluster(t, "", lists())
	refusedAtBound(t, "no filer listed", c)
	if n := ms[0].callCount(); n < 3 {
		t.Fatalf("no filer listed: %d rounds; want retries until the bound", n)
	}
	f := &configFiler{}
	c, _ = startConfigFiler(t, f)
	f.mu.Lock()
	f.masters = []string{deadAddress(t)}
	f.mu.Unlock()
	refusedAtBound(t, "unreachable master", c)
	c, _ = startConfigFiler(t, &configFiler{err: status.Error(codes.Unimplemented, "no")})
	if err := c.CheckSingleFiler(context.Background()); !errors.Is(err, ErrNotSingleFiler) {
		t.Fatalf("filer without configuration: %v; want a refusal", err)
	}
	c, _ = startConfigFiler(t, &configFiler{})
	if err := c.CheckSingleFiler(context.Background()); !errors.Is(err, ErrNotSingleFiler) {
		t.Fatalf("filer with no master: %v; want a refusal", err)
	}
}

func TestGRPCAddress(t *testing.T) {
	for in, want := range map[string]string{
		"m1:9333":          "m1:19333",
		"m1:9333.19555":    "m1:19555",
		"10.0.0.5:9333":    "10.0.0.5:19333",
		"[::1]:9333":       "[::1]:19333",
		"m1:9333.notaport": "",
		"m1":               "",
		"m1:60000":         "",
	} {
		got, err := grpcAddress(in)
		if want == "" {
			if err == nil {
				t.Errorf("%q: got %q, want an error", in, got)
			}
			continue
		}
		if err != nil || got != want {
			t.Errorf("%q: got %q %v, want %q", in, got, err, want)
		}
	}
}
