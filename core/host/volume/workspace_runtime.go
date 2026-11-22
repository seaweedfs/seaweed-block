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
			id := &control.WorkspaceIdentity{VolumeId: h.cfg.VolumeID, ReplicaId: h.cfg.ReplicaID, ServerId: h.cfg.ServerID, Epoch: p.Epoch}
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
// (its chunk layer is being detached): the lease guard renews at once and
// then sleeps at most stopPendingPoll between renewals, instead of its normal
// renewal delay, so it sees the revocation that follows sooner. Scheduling
// only: what the guard admits or ACKs is unchanged. It returns this detach
// attempt's token for WorkspaceStopResumed; other volume ids are ignored
// (token 0). Never blocks.
func (h *Host) WorkspaceStopPending(volumeID string) uint64 {
	if volumeID != h.cfg.VolumeID {
		return 0
	}
	h.workspaceLeaseMu.Lock()
	defer h.workspaceLeaseMu.Unlock()
	h.workspaceStopAttempt++
	h.workspaceStopPending = true
	if g := h.view.workspaceGuard.Load(); g != nil {
		g.StopPending()
	}
	return h.workspaceStopAttempt
}

// WorkspaceStopResumed is called when the detach attempt that returned
// attempt provably did not happen and the volume serves again: stop-pending
// ends and the guard returns to its normal renewal delay. A token other than
// the latest attempt's (an older attempt, 0, another volume) changes nothing,
// so an old attempt's resume never clears a newer pending detach. Never blocks.
func (h *Host) WorkspaceStopResumed(volumeID string, attempt uint64) {
	if volumeID != h.cfg.VolumeID || attempt == 0 {
		return
	}
	h.workspaceLeaseMu.Lock()
	defer h.workspaceLeaseMu.Unlock()
	if !h.workspaceStopPending || attempt != h.workspaceStopAttempt {
		return
	}
	h.workspaceStopPending = false
	if g := h.view.workspaceGuard.Load(); g != nil {
		g.ClearStopPending()
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
