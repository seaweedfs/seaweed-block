package chunkstore

import (
	"context"
	"errors"
	"net"
	"strings"
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
	masters []string
	group   string
	err     error
}

func (f *configFiler) GetFilerConfiguration(context.Context, *filerpb.GetFilerConfigurationRequest) (*filerpb.GetFilerConfigurationResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &filerpb.GetFilerConfigurationResponse{Masters: f.masters, FilerGroup: f.group}, nil
}

type nodesMaster struct {
	masterpb.UnimplementedSeaweedServer
	filers []string
	emptyN int // the first emptyN calls list no filer (not registered yet)
	calls  int
	got    *masterpb.ListClusterNodesRequest
}

func (m *nodesMaster) ListClusterNodes(_ context.Context, r *masterpb.ListClusterNodesRequest) (*masterpb.ListClusterNodesResponse, error) {
	m.got = r
	m.calls++
	resp := &masterpb.ListClusterNodesResponse{}
	if m.calls <= m.emptyN {
		return resp, nil
	}
	for _, a := range m.filers {
		resp.ClusterNodes = append(resp.ClusterNodes, &masterpb.ListClusterNodesResponse_ClusterNode{Address: a})
	}
	return resp, nil
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

func startConfigFiler(t *testing.T, f *configFiler) *SWFS {
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
	return c
}

// Block metadata needs exactly one filer: a group with one filer passes, also
// when the filer registers with the master only after a few rounds; two
// filers are refused at once; no filer, an unreachable master or a filer that
// cannot report its configuration are refused at the bound (fail closed).
func TestCheckSingleFiler(t *testing.T) {
	oldW, oldR := singleFilerWait, singleFilerRetry
	singleFilerWait, singleFilerRetry = 1500*time.Millisecond, 100*time.Millisecond
	t.Cleanup(func() { singleFilerWait, singleFilerRetry = oldW, oldR })
	ctx := context.Background()

	one := &nodesMaster{filers: []string{"10.0.0.1:8888"}}
	c := startConfigFiler(t, &configFiler{masters: []string{startMaster(t, one)}, group: "g1"})
	if err := c.CheckSingleFiler(ctx); err != nil {
		t.Fatalf("one filer refused: %v", err)
	}
	if one.got.GetClientType() != "filer" || one.got.GetFilerGroup() != "g1" {
		t.Fatalf("master asked for %q in group %q; want filer in g1", one.got.GetClientType(), one.got.GetFilerGroup())
	}

	// A fresh filer registers with the master only after it serves.
	late := &nodesMaster{filers: []string{"10.0.0.1:8888"}, emptyN: 3}
	c = startConfigFiler(t, &configFiler{masters: []string{startMaster(t, late)}})
	if err := c.CheckSingleFiler(ctx); err != nil || late.calls != 4 {
		t.Fatalf("filer listed on the 4th round: %v after %d calls; want a pass", err, late.calls)
	}

	two := &nodesMaster{filers: []string{"10.0.0.1:8888", "10.0.0.2:8888"}}
	c = startConfigFiler(t, &configFiler{masters: []string{startMaster(t, two)}})
	start := time.Now()
	err := c.CheckSingleFiler(ctx)
	if !errors.Is(err, ErrNotSingleFiler) || !strings.Contains(err.Error(), "10.0.0.2:8888") || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("two filers: %v after %s; want ErrNotSingleFiler naming both, at once", err, time.Since(start))
	}

	none := &nodesMaster{}
	c = startConfigFiler(t, &configFiler{masters: []string{startMaster(t, none)}})
	start = time.Now()
	if err := c.CheckSingleFiler(ctx); !errors.Is(err, ErrNotSingleFiler) || time.Since(start) < singleFilerWait || none.calls < 3 {
		t.Fatalf("no filer listed: %v after %s and %d calls; want retries, then a refusal at the bound", err, time.Since(start), none.calls)
	}

	start = time.Now()
	c = startConfigFiler(t, &configFiler{masters: []string{deadAddress(t)}})
	if err := c.CheckSingleFiler(ctx); !errors.Is(err, ErrNotSingleFiler) || time.Since(start) > 5*time.Second {
		t.Fatalf("unreachable master: %v after %s; want a bounded refusal", err, time.Since(start))
	}

	c = startConfigFiler(t, &configFiler{err: status.Error(codes.Unimplemented, "no")})
	if err := c.CheckSingleFiler(ctx); !errors.Is(err, ErrNotSingleFiler) {
		t.Fatalf("filer without configuration: %v; want a refusal", err)
	}

	c = startConfigFiler(t, &configFiler{})
	if err := c.CheckSingleFiler(ctx); !errors.Is(err, ErrNotSingleFiler) {
		t.Fatalf("filer with no master: %v; want a refusal", err)
	}

	// An unreachable first master does not hide the answer of the next one.
	c = startConfigFiler(t, &configFiler{masters: []string{deadAddress(t), startMaster(t, &nodesMaster{filers: []string{"10.0.0.1:8888"}})}})
	if err := c.CheckSingleFiler(ctx); err != nil {
		t.Fatalf("second master answers one filer: %v", err)
	}
}

func TestMasterGRPCAddress(t *testing.T) {
	for in, want := range map[string]string{
		"m1:9333":          "m1:19333",
		"m1:9333.19555":    "m1:19555",
		"10.0.0.5:9333":    "10.0.0.5:19333",
		"[::1]:9333":       "[::1]:19333",
		"m1:9333.notaport": "",
		"m1":               "",
		"m1:60000":         "",
	} {
		got, err := masterGRPCAddress(in)
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
