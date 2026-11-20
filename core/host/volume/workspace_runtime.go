package volume

import (
	"context"
	"fmt"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"time"
)

// RunWorkspaceLease is the one-shot workspace lease lifecycle for this process.
// The view is gated from Host.New, including the interval before assignment or
// the first lease. A new assignment after fencing requires a new process.
// Stop must drain/close frontend, capture and replication, then observe the
// stored detached HEAD. It must not take a final cut on revocation or expiry.
func (h *Host) RunWorkspaceLease(ctx context.Context, stop func(context.Context, *control.WorkspaceIdentity) (uint64, error)) error {
	return h.runWorkspaceLease(ctx, stop, nil)
}

// beforeInstall is a deterministic test seam before guard publication.
func (h *Host) runWorkspaceLease(ctx context.Context, stop func(context.Context, *control.WorkspaceIdentity) (uint64, error), beforeInstall func()) error {
	if !h.cfg.WorkspaceLease || stop == nil {
		return fmt.Errorf("workspace lease runtime is not configured")
	}
	if !h.workspaceLeaseStarted.CompareAndSwap(false, true) {
		return fmt.Errorf("workspace lease runtime already started")
	}
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		p := h.view.Projection()
		if p.Epoch > 0 {
			id := &control.WorkspaceIdentity{VolumeId: h.cfg.VolumeID, ReplicaId: h.cfg.ReplicaID, Epoch: p.Epoch}
			guard, err := NewWorkspaceLeaseGuard(h.view, control.NewWorkspaceServiceClient(h.conn), id, func(c context.Context) (uint64, error) { return stop(c, id) }, nil)
			if err != nil {
				return err
			}
			if beforeInstall != nil {
				beforeInstall()
			}
			h.workspaceLeaseMu.Lock()
			// The guard is not yet shared: publish it already fenced if a
			// synchronous shutdown won the race with this installer.
			guard.fenced = h.workspaceLeaseFenced || ctx.Err() != nil
			guard.stopPending = h.workspaceStopPending
			h.view.workspaceGuard.Store(guard)
			h.workspaceLeaseMu.Unlock()
			return guard.Run(ctx)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// WorkspaceStopPending is called when a graceful stop of volumeID has begun
// (its chunk layer is being detached): the lease guard then polls the master
// at a short cadence so it fences soon after the revocation instead of at its
// next normal renewal. Other volume ids are ignored. Never blocks.
func (h *Host) WorkspaceStopPending(volumeID string) {
	if volumeID != h.cfg.VolumeID {
		return
	}
	h.workspaceLeaseMu.Lock()
	defer h.workspaceLeaseMu.Unlock()
	h.workspaceStopPending = true
	if g := h.view.workspaceGuard.Load(); g != nil {
		g.StopPending()
	}
}

// FenceWorkspaceLease closes admission synchronously before process shutdown.
// Physical drain remains owned by run's stop callback; this never ACKs/cuts.
func (h *Host) FenceWorkspaceLease() {
	h.workspaceLeaseMu.Lock()
	defer h.workspaceLeaseMu.Unlock()
	h.workspaceLeaseFenced = true
	if g := h.view.workspaceGuard.Load(); g != nil {
		g.mu.Lock()
		g.fenced = true
		g.mu.Unlock()
	}
}
