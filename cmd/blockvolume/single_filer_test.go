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

// blockvolume's chunk wiring runs the single-filer check at startup: the
// configured filer listed as the one filer passes; a group with a second
// filer is refused.
func TestChunkWiringRefusesAMultiFilerGroup(t *testing.T) {
	for _, c := range []struct {
		name  string
		other []string
		ok    bool
	}{
		{"one filer", nil, true},
		{"two filers", []string{"10.0.0.2:8888"}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			filer := lis.Addr().String()
			_, fport, _ := net.SplitHostPort(filer)
			self := "127.0.0.1:8888." + fport // the filer as a master lists it
			m := serve(t, func(s *grpc.Server) {
				masterpb.RegisterSeaweedServer(s, &groupMaster{filers: append([]string{self}, c.other...)})
			})
			_, mport, _ := net.SplitHostPort(m)
			srv := grpc.NewServer()
			filerpb.RegisterSeaweedFilerServer(srv, &groupFiler{master: "127.0.0.1:9333." + mport})
			go func() { _ = srv.Serve(lis) }()
			t.Cleanup(srv.Stop)
			w, err := newChunkWiring(chunkFlags{filer: filer, path: defaultChunkPath}, nil)
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
