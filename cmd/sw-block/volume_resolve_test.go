package main

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type volumeResolveServer struct {
	control.UnimplementedLifecycleServiceServer
	mu    sync.Mutex
	reply *control.CreateVolumeResponse
	err   error
	names []string
}

func (s *volumeResolveServer) GetVolume(_ context.Context, q *control.GetLifecycleVolumeRequest) (*control.CreateVolumeResponse, error) {
	s.mu.Lock()
	s.names = append(s.names, q.GetVolumeId())
	s.mu.Unlock()
	return s.reply, s.err
}
func serveVolumeResolver(t *testing.T, s *volumeResolveServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	control.RegisterLifecycleServiceServer(server, s)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	return lis.Addr().String()
}
func TestOpsVolumeResolvePrintsOnlyMasterLayoutID(t *testing.T) {
	const id = "1234567890abcdef1234567890abcdef"
	s := &volumeResolveServer{reply: &control.CreateVolumeResponse{VolumeId: "v1", LayoutId: id}}
	addr := serveVolumeResolver(t, s)
	var out, errout bytes.Buffer
	code := run([]string{"ops", "volume", "resolve", "--master", addr, "v1"}, &out, &errout)
	if code != 0 || out.String() != id+"\n" || errout.Len() != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, &out, &errout)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.names) != 1 || s.names[0] != "v1" {
		t.Fatalf("resolved names=%v", s.names)
	}
}

func TestOpsVolumeResolveRefusesMissingOrForeignIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, volume, id string
		err              error
	}{
		{"missing", "", "", status.Error(codes.NotFound, "no such volume")},
		{"unavailable", "", "", status.Error(codes.Unavailable, "master unavailable")},
		{"invalid-id", "v1", "bad", nil},
		{"wrong-name", "v2", "1234567890abcdef1234567890abcdef", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &volumeResolveServer{reply: &control.CreateVolumeResponse{VolumeId: tc.volume, LayoutId: tc.id}, err: tc.err}
			addr := serveVolumeResolver(t, s)
			var out, errout bytes.Buffer
			code := run([]string{"ops", "volume", "resolve", "--master", addr, "v1"}, &out, &errout)
			if code != 2 || out.Len() != 0 || errout.Len() == 0 {
				t.Fatalf("code=%d stdout=%q stderr=%q", code, &out, &errout)
			}
		})
	}
}
func TestOpsVolumeResolveRequiresOneNameAndBoundedQuery(t *testing.T) {
	for _, args := range [][]string{
		{}, {"other"}, {"resolve", "v1"}, {"resolve", "--master", "unused"},
		{"resolve", "--master", "unused", "v1", "v2"},
		{"resolve", "--master", "unused", "--timeout", "0s", "v1"},
	} {
		var out, errout bytes.Buffer
		code := run(append([]string{"ops", "volume"}, args...), &out, &errout)
		if code != 2 || out.Len() != 0 || errout.Len() == 0 {
			t.Fatalf("args=%v code=%d stdout=%q stderr=%q", args, code, &out, &errout)
		}
	}
}
