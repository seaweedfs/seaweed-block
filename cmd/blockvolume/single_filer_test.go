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
	conf   string // /etc/seaweedfs/filer.conf
}

// fsyncConf is the rule the filer writes for fs.configure -locationPrefix=/swblock/ -fsync.
const fsyncConf = `{"version":1,"locations":[{"locationPrefix":"/swblock/","fsync":true}]}`

func (f *groupFiler) LookupDirectoryEntry(_ context.Context, r *filerpb.LookupDirectoryEntryRequest) (*filerpb.LookupDirectoryEntryResponse, error) {
	if r.GetDirectory() != "/etc/seaweedfs" || r.GetName() != "filer.conf" {
		return nil, errors.New("unexpected lookup " + r.GetDirectory() + "/" + r.GetName())
	}
	return &filerpb.LookupDirectoryEntryResponse{Entry: &filerpb.Entry{Name: "filer.conf", Content: []byte(f.conf),
		Attributes: &filerpb.FuseAttributes{FileSize: uint64(len(f.conf))}}}, nil
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

// blockvolume's chunk wiring runs the single-filer check and the chunk path
// rule check at startup: the configured filer listed as the one filer, with
// an fsync rule on the chunk path, passes; a group with a second filer is
// refused, and so is a chunk path whose rule does not set fsync.
func TestChunkWiringRefusesAMultiFilerGroup(t *testing.T) {
	for _, c := range []struct {
		name  string
		other []string
		conf  string
		want  error // nil passes
	}{
		{"one filer", nil, fsyncConf, nil},
		{"two filers", []string{"10.0.0.2:8888"}, fsyncConf, chunkstore.ErrNotSingleFiler},
		{"no fsync rule", nil, `{"version":1,"locations":[{"locationPrefix":"/swblock/","collection":"c"}]}`, chunkstore.ErrPathRule},
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
			filerpb.RegisterSeaweedFilerServer(srv, &groupFiler{master: "127.0.0.1:9333." + mport, conf: c.conf})
			go func() { _ = srv.Serve(lis) }()
			t.Cleanup(srv.Stop)
			w, err := newChunkWiring(chunkFlags{filer: filer, path: defaultChunkPath})
			if c.want == nil {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				_ = w.close()
				return
			}
			if !errors.Is(err, c.want) {
				t.Fatalf("got %v; want %v", err, c.want)
			}
		})
	}
}
