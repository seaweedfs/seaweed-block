package main

import (
	"context"
	"errors"
	"net"
	"testing"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/masterpb"
)

type groupFiler struct {
	filerpb.UnimplementedSeaweedFilerServer
	master string
}

func (f *groupFiler) GetFilerConfiguration(context.Context, *filerpb.GetFilerConfigurationRequest) (*filerpb.GetFilerConfigurationResponse, error) {
	return &filerpb.GetFilerConfigurationResponse{Masters: []string{f.master}}, nil
}

type groupMaster struct {
	masterpb.UnimplementedSeaweedServer
	filers []string
}

func (m *groupMaster) ListClusterNodes(context.Context, *masterpb.ListClusterNodesRequest) (*masterpb.ListClusterNodesResponse, error) {
	r := &masterpb.ListClusterNodesResponse{}
	for _, a := range m.filers {
		r.ClusterNodes = append(r.ClusterNodes, &masterpb.ListClusterNodesResponse_ClusterNode{Address: a})
	}
	return r, nil
}

func serve(t *testing.T, register func(*grpc.Server)) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// blockvolume's chunk wiring runs the single-filer check at startup: a filer
// group with two filers is refused, one filer passes.
func TestChunkWiringRefusesAMultiFilerGroup(t *testing.T) {
	for _, c := range []struct {
		name   string
		filers []string
		ok     bool
	}{
		{"one filer", []string{"10.0.0.1:8888"}, true},
		{"two filers", []string{"10.0.0.1:8888", "10.0.0.2:8888"}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			m := serve(t, func(s *grpc.Server) { masterpb.RegisterSeaweedServer(s, &groupMaster{filers: c.filers}) })
			_, mport, _ := net.SplitHostPort(m)
			filer := serve(t, func(s *grpc.Server) {
				filerpb.RegisterSeaweedFilerServer(s, &groupFiler{master: "127.0.0.1:9333." + mport})
			})
			w, err := newChunkWiring(chunkFlags{filer: filer, path: defaultChunkPath})
			if c.ok {
				if err != nil {
					t.Fatalf("one filer refused: %v", err)
				}
				_ = w.close()
				return
			}
			if !errors.Is(err, chunkstore.ErrNotSingleFiler) {
				t.Fatalf("two filers: %v; want chunkstore.ErrNotSingleFiler", err)
			}
		})
	}
}
