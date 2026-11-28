package main

import (
	"bytes"
	"context"
	"net"
	"reflect"
	"sync"
	"testing"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The S1 command's public seam: a running master's existing lifecycle and
// workspace RPCs. This receiver checks the operator's intent, not private CLI
// functions. No test-only command implementation stands in for the command.
type createAndPlaceReceiver struct {
	control.UnimplementedLifecycleServiceServer
	control.UnimplementedWorkspaceServiceServer
	mu    sync.Mutex
	calls []string
}

func (s *createAndPlaceReceiver) CreateVolume(_ context.Context, q *control.CreateVolumeRequest) (*control.CreateVolumeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if q.GetVolumeId() != "new-volume" || q.GetSizeBytes() != 268435456 || q.GetReplicationFactor() != 1 || q.GetProtocol() != "nvme" || q.GetFrontendTransport() != "tcp" || q.GetSourceVolume() != "" || q.GetSourceSnapshot() != "" || q.GetCreateRequestId() != "" {
		return nil, status.Error(codes.InvalidArgument, "wrong ordinary RF1 create intent")
	}
	s.calls = append(s.calls, "create")
	return &control.CreateVolumeResponse{VolumeId: q.VolumeId, SizeBytes: q.SizeBytes, ReplicationFactor: 1, Protocol: "nvme", FrontendTransport: "tcp", LayoutId: "1234567890abcdef1234567890abcdef", BlockSize: 4096, ChunkBytes: 4194304, StoreReady: true}, nil
}

func (s *createAndPlaceReceiver) RegisterWorkspaceBinding(_ context.Context, q *control.WorkspaceBinding) (*control.WorkspaceBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !reflect.DeepEqual(s.calls, []string{"create"}) || q.GetVolumeId() != "new-volume" || q.GetServerId() != "registered-host" || q.GetDataAddr() != "192.0.2.10:9201" || q.GetCtrlAddr() != "192.0.2.10:9202" || q.GetRetired() {
		return nil, status.Error(codes.FailedPrecondition, "binding must follow store-ready create with exact endpoints")
	}
	s.calls = append(s.calls, "bind")
	return q, nil
}

func (s *createAndPlaceReceiver) PlaceWorkspace(_ context.Context, q *control.WorkspacePlacement) (*control.WorkspaceEmpty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !reflect.DeepEqual(s.calls, []string{"create", "bind"}) || q.GetVolumeId() != "new-volume" || q.GetServerId() != "registered-host" || q.GetReplicaId() != "r1" {
		return nil, status.Error(codes.FailedPrecondition, "placement must follow the exact prepared binding")
	}
	s.calls = append(s.calls, "place")
	return &control.WorkspaceEmpty{}, nil
}

func TestOpsVolumeCreateAndPlaceWithoutMasterRestart(t *testing.T) {
	for _, key := range []string{"SW_BLOCK_TLS_CERT", "SW_BLOCK_TLS_KEY", "SW_BLOCK_TLS_CA"} {
		t.Setenv(key, "")
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &createAndPlaceReceiver{}
	server := grpc.NewServer()
	control.RegisterLifecycleServiceServer(server, s)
	control.RegisterWorkspaceServiceServer(server, s)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	var out, errout bytes.Buffer
	code := run([]string{"ops", "volume", "create", "--master", lis.Addr().String(), "--size-bytes", "268435456", "--protocol", "nvme", "--transport", "tcp", "--server-id", "registered-host", "--replica-id", "r1", "--data-addr", "192.0.2.10:9201", "--ctrl-addr", "192.0.2.10:9202", "--timeout", "5s", "new-volume"}, &out, &errout)
	if code != 0 {
		t.Fatalf("online create-and-place: exit=%d stdout=%q stderr=%q", code, &out, &errout)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !reflect.DeepEqual(s.calls, []string{"create", "bind", "place"}) {
		t.Fatalf("master RPCs=%v; want one create, binding, placement on the running master", s.calls)
	}
}
