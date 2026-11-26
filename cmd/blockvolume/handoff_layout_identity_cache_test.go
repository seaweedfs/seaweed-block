//go:build subprocess && chunkstore_integration

package main_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
)

type handoffIdentityServer struct {
	control.UnimplementedLifecycleServiceServer
	calls atomic.Int32
}

func (s *handoffIdentityServer) GetVolume(_ context.Context, req *control.GetLifecycleVolumeRequest) (*control.CreateVolumeResponse, error) {
	s.calls.Add(1)
	return &control.CreateVolumeResponse{VolumeId: req.VolumeId, LayoutId: "00112233445566778899aabbccddeeff"}, nil
}

// Known-zero's source and target env constructors begin without a cache.
// Resolve through the real helper/RPC, then require cached reads to keep
// working when the master is unavailable (the crash fixtures depend on it).
func TestHandoffLayoutIdentityCacheStartsEmpty(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	rpc := &handoffIdentityServer{}
	control.RegisterLifecycleServiceServer(server, rpc)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	env := &handoffEnv{h: &plannedHandoffHarness{t: t, masterAddr: listener.Addr().String()}}
	const want = "00112233445566778899aabbccddeeff"
	for _, volume := range []string{"kzext4", "kzxfs", "kzext4", "kzxfs"} {
		if got := env.layoutID(volume); got != want {
			t.Fatalf("%s id=%s want %s", volume, got, want)
		}
	}
	if got := rpc.calls.Load(); got != 2 {
		t.Fatalf("resolve calls=%d want two distinct volumes", got)
	}
	server.Stop()
	if got := env.layoutID("kzext4"); got != want {
		t.Fatalf("cached id after master stop=%s", got)
	}
}
