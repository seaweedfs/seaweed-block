package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

const (
	exitWorkspaceRetry      = 75
	exitWorkspaceSuperseded = 78
)

type workspaceExit struct {
	code   int
	reason string
}

// INV-WS-STARTUP-RETRY-001: the first outcome is immutable. The channel is a
// single latched wakeup, not a queue of competing shutdown events.
type processExit struct {
	parent  context.Context
	once    sync.Once
	done    chan struct{}
	outcome workspaceExit
}

func newProcessExit(ctx context.Context) *processExit {
	return &processExit{parent: ctx, done: make(chan struct{})}
}
func (e *processExit) choose(out workspaceExit) {
	e.once.Do(func() {
		if e.parent.Err() != nil {
			out = workspaceExit{reason: "signal"}
		}
		e.outcome = out
		close(e.done)
	})
}

// The original resource owner still drains everything. This outer process
// owner bounds even a stuck Close or deferred cleanup; main alone calls Exit.
func run(f flags) int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return superviseProcess(ctx, func(e *processExit) int { return runVolume(f, e) })
}

// INV-WS-STARTUP-RETRY-001: M2 process exit owns the cleanup bound.
func superviseProcess(ctx context.Context, work func(*processExit) int) int {
	e := newProcessExit(ctx)
	finished := make(chan int, 1)
	go func() { finished <- work(e) }()
	select {
	case code := <-finished:
		select {
		case <-e.done:
			if e.outcome.code == 0 {
				return code
			}
			return reportWorkspaceExit(e.outcome)
		default:
			return code
		}
	case <-ctx.Done():
		e.choose(workspaceExit{reason: "signal"})
	case <-e.done:
	}
	if e.outcome.code == 0 {
		return <-finished
	}
	timer := time.NewTimer(volume.WorkspaceDrainTimeout)
	defer timer.Stop()
	return finishWorkspaceExit(e.outcome, finished, timer.C)
}

func finishWorkspaceExit(out workspaceExit, finished <-chan int, deadline <-chan time.Time) int {
	select {
	case <-finished:
	case <-deadline:
	}
	return reportWorkspaceExit(out)
}

func reportWorkspaceExit(out workspaceExit) int {
	if out.code != 0 {
		fmt.Fprintf(os.Stderr, "blockvolume: exiting status=%d reason=%s\n", out.code, out.reason)
	}
	return out.code
}

// INV-WS-STARTUP-RETRY-001: only a positive, volume-specific other-replica
// fact permits no-retry. A
// higher epoch for the same replica must restart for the new assignment.
func workspaceRetryOutcome(volumeID, replicaID, reason string, other *control.AssignmentFact) workspaceExit {
	if other != nil && other.VolumeId == volumeID && other.ReplicaId != "" && other.ReplicaId != replicaID {
		return workspaceExit{exitWorkspaceSuperseded, "superseded"}
	}
	return workspaceExit{exitWorkspaceRetry, reason}
}

// M2 startup outcome; INV-WS-STARTUP-RETRY-001. Workspace chunk primaries only.
func watchChunkStartup(ctx context.Context, e *processExit, f flags, h *volume.Host, p *durable.DurableProvider) {
	if !f.workspaceLease || f.durableImpl != "walstore-chunk" {
		return
	}
	for {
		if chooseChunkStartupExit(e, f, h, p) {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-e.done:
			return
		case <-p.ChunkStartupUpdates():
		}
	}
}

// M2 process exit / Revoking -> Revoked; INV-WS-STARTUP-RETRY-001.
func waitWorkspaceStop(e *processExit, f flags, h *volume.Host, requests <-chan workspaceStopRequest, done <-chan error) (*workspaceStopRequest, <-chan error) {
	select {
	case <-e.parent.Done():
		e.choose(workspaceExit{reason: "signal"})
		return nil, done
	case <-e.done:
		return nil, done
	case request := <-requests:
		out := workspaceExit{reason: "revoked"}
		if !h.WorkspaceLeaseRevoking() {
			out = workspaceRetryOutcome(f.volumeID, f.replicaID, "lease_expired", h.LastOtherLine())
		}
		if out.code != 0 {
			e.choose(out)
		}
		return &request, done
	case err := <-done:
		fmt.Fprintln(os.Stderr, "blockvolume: workspace lease stopped:", err)
		e.choose(workspaceRetryOutcome(f.volumeID, f.replicaID, "lease_expired", h.LastOtherLine()))
		return nil, nil
	}
}

// An ACKed revocation completes with status zero. A failure to confirm that
// ACK is retryable; a signal observed first still retains the signal status.
// M2 Revoking -> Revoked; INV-WS-REVOKE-ACK-001, INV-WS-STARTUP-RETRY-001.
func finishWorkspaceLease(e *processExit, f flags, h *volume.Host, done <-chan error) {
	if done == nil {
		return
	}
	select {
	case err := <-done:
		if err == nil {
			return
		}
		fmt.Fprintln(os.Stderr, "blockvolume: workspace lease closeout:", err)
	case <-time.After(3 * time.Second):
		fmt.Fprintln(os.Stderr, "blockvolume: workspace lease closeout deadline")
	}
	e.choose(workspaceRetryOutcome(f.volumeID, f.replicaID, "lease_expired", h.LastOtherLine()))
}

func chooseChunkStartupExit(e *processExit, f flags, h *volume.Host, p *durable.DurableProvider) bool {
	if !f.workspaceLease || f.durableImpl != "walstore-chunk" {
		return false
	}
	gen, failed := p.ChunkStartupOutcome(f.volumeID)
	if !failed {
		return false
	}
	out := workspaceRetryOutcome(f.volumeID, f.replicaID, "assignment_timeout", h.LastOtherLine())
	return p.ChooseChunkStartupExit(f.volumeID, gen, func() { e.choose(out) })
}
