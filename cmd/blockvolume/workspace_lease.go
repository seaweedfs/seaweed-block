package main

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type workspaceStopResult struct {
	gen uint64
	err error
}
type workspaceStopRequest struct {
	ctx   context.Context
	id    *control.WorkspaceIdentity
	reply chan workspaceStopResult
}

// The lease worker requests closure; only run's owner closes the actual
// frontends, replication and provider. Signal cancellation suppresses ACK even
// when it races an already-requested revocation drain.
func startWorkspaceLease(ctx context.Context, h *volume.Host) (<-chan workspaceStopRequest, <-chan error) {
	requests := make(chan workspaceStopRequest)
	done := make(chan error, 1)
	go func() {
		done <- h.RunWorkspaceLease(ctx, func(stopCtx context.Context, id *control.WorkspaceIdentity) (uint64, error) {
			req := workspaceStopRequest{ctx: stopCtx, id: id, reply: make(chan workspaceStopResult, 1)}
			select {
			case requests <- req:
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-stopCtx.Done():
				return 0, stopCtx.Err()
			}
			select {
			case result := <-req.reply:
				return result.gen, result.err
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-stopCtx.Done():
				return 0, stopCtx.Err()
			}
		})
	}()
	return requests, done
}

// CLEAN requires a prior Detach while the lease was valid. This is read-only:
// revocation, expiry and cancellation never manufacture a final cut/proof.
func workspaceDetachedGeneration(ctx context.Context, remoteFor func(string) (storage.ChunkRemote, error), id *control.WorkspaceIdentity, holder string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if remoteFor == nil || id == nil {
		return 0, fmt.Errorf("workspace remote/identity missing")
	}
	remote, err := remoteFor(id.VolumeId)
	if err != nil {
		return 0, err
	}
	head, err := storage.ReadDetachedChunkHead(ctx, remote, id.Epoch, holder)
	if err != nil {
		return 0, err
	}
	return head.Gen, nil
}
