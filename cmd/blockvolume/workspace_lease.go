package main

import (
	"context"
	"errors"
	"fmt"

	holderid "github.com/seaweedfs/seaweed-block/core/holder"
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

// rootProvider is the part of the durable provider the shutdown reads.
type rootProvider interface {
	LocalRootNonce(volumeID string) (string, error)
	Close() error
}

// closeDurableKeepingRoot reads this incarnation's local root nonce, for the
// revocation ACK's proof, and then closes the provider: Close forgets the
// volume, so the order is fixed here (V1-SPEC r2.38). A nonce that cannot be
// read is returned with the close error.
func closeDurableKeepingRoot(p rootProvider, volumeID string) (string, error) {
	root, rootErr := p.LocalRootNonce(volumeID)
	return root, errors.Join(rootErr, p.Close())
}

// CLEAN requires a prior Detach while the lease was valid. This is read-only:
// revocation, expiry and cancellation never manufacture a final cut/proof.
func workspaceDetachedGeneration(ctx context.Context, remoteFor func(string) (storage.ChunkRemote, error), id *control.WorkspaceIdentity, holder, root string) (uint64, error) {
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
	head, err := remote.GetHead(ctx)
	if err != nil {
		return 0, err
	}
	proof := head.DetachedBy
	if head.Gen == 0 || head.Epoch != id.Epoch || !holderid.Claimable(head.Holder, head.HolderRoot, holder, root) || head.State != storage.ChunkHeadDetached || proof == nil || proof.Gen != head.Gen || proof.Epoch != head.Epoch || !holderid.Same(proof.Holder, proof.HolderRoot, head.Holder, head.HolderRoot) {
		return 0, fmt.Errorf("workspace shutdown has no matching stored detached HEAD")
	}
	return head.Gen, nil
}
