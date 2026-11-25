package main

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
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

// workspaceLayoutDetachedGeneration accepts only the attached Start identity's
// exact detached HEAD. Missing, foreign or unknown evidence never yields ACK.
// The returned generation is diagnostic readout, not the clean-move identity:
// the master reads this detached epoch/holder/position when recording the ACK.
func workspaceLayoutDetachedGeneration(ctx context.Context, st layout.HeadStore, id *control.WorkspaceIdentity, me layout.HolderID) (uint64, error) {
	if id == nil || me.Epoch == 0 || id.Epoch != me.Epoch || me.Holder == "" || id.ServerId+"/"+id.ReplicaId != me.Holder || me.ClaimID == "" {
		return 0, layout.ErrNotHolder
	}
	h, found, err := st.ReadHead(ctx)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, layout.ErrHeadAbsent
	}
	if !layout.DetachedBy(h, me) {
		return 0, fmt.Errorf("workspace: no detached proof for the attached claim")
	}
	return h.Gen, nil
}
