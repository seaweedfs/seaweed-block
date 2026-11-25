package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/host/master"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// The real command reserves through the real master while a first lease
// request is paused at RPC ingress. At the command's CAS-time lease check,
// resume that request: committed exclusion must refuse it before the check
// can certify absence. The authority package separately pauses every writer
// immediately before its grant lock; this test composes the transport path.
func TestOpsChunkVolumeDeleteClosesAPausedFirstGrant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	paused, resume := make(chan struct{}), make(chan struct{})
	grantResult := make(chan error, 1)
	var checked atomic.Bool
	intercept := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		switch info.FullMethod {
		case control.WorkspaceService_RenewWorkspaceLease_FullMethodName:
			close(paused)
			select {
			case <-resume:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		case control.LifecycleService_CheckVolumeNoLiveLease_FullMethodName:
			close(resume)
			select {
			case err := <-grantResult:
				if status.Code(err) != codes.FailedPrecondition {
					return nil, status.Errorf(codes.Internal, "paused grant escaped reservation: %v", err)
				}
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			checked.Store(true)
		}
		return next(ctx, req)
	}
	cfg := master.Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true,
		ServerOptions:       []grpc.ServerOption{grpc.ChainUnaryInterceptor(intercept)},
		WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) { return authority.WorkspaceHead{}, nil }}
	h, stop := startDeleteMaster(t, cfg)
	defer stop()
	seedForcedVolumeLease(t, h, false)
	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	go func() {
		_, err := control.NewWorkspaceServiceClient(conn).RenewWorkspaceLease(ctx, &control.WorkspaceIdentity{ServerId: "node-a", VolumeId: "vol-f", ReplicaId: "r1", Epoch: 1})
		grantResult <- err
	}()
	select {
	case <-paused:
	case <-ctx.Done():
		t.Fatal("first grant did not reach RPC ingress")
	}
	store := newMemChunkStore()
	useMemChunkStore(t, store)
	v := cliVolume(t, store, "vol-f", "daily")
	code, out, stderr := runChunk(cliArgs("volume-delete", volArgs("volume", v), []string{"--master", h.Addr(), "--request-id", "paused-first-grant"})...)
	if code != 0 || !checked.Load() {
		t.Fatalf("delete exit=%d checked=%v out=%s err=%s", code, checked.Load(), out, stderr)
	}
	cat, err := chunkstore.NewBCatalog(store, store, defaultChunkPathCLI)
	if err != nil {
		t.Fatal(err)
	}
	head, found, err := cat.ReadHead(ctx, v)
	if err != nil || !found || head.State != layout.StateDeleted {
		t.Fatalf("final HEAD: %+v found=%v err=%v", head, found, err)
	}
	if !h.Lifecycle().Deletions.CommittedSnapshot("vol-f") {
		t.Fatal("command did not commit the real master's reservation")
	}
	if s := h.Publisher().WorkspaceState("vol-f"); s != nil && s.Lease != nil {
		t.Fatalf("late request created a lease: %+v", s.Lease)
	}
}
