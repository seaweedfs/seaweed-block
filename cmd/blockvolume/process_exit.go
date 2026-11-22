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
)

const (
	exitWorkspaceRetry      = 75
	exitWorkspaceSuperseded = 78
	// Existing post-drain ACK wait; M2 Revoking -> Revoked, Part D.
	workspaceLeaseCloseoutTimeout = 3 * time.Second
)

type workspaceExit struct {
	code   int
	reason string
}

// INV-WS-STARTUP-RETRY-001: the first outcome is immutable. The channel is a
// single latched wakeup, not a queue of competing shutdown events.
type processExit struct {
	parent   context.Context
	once     sync.Once
	done     chan struct{}
	decision chan workspaceExit
}

func newProcessExit(ctx context.Context) *processExit {
	return &processExit{parent: ctx, done: make(chan struct{}), decision: make(chan workspaceExit, 1)}
}
func (e *processExit) choose(out workspaceExit) {
	e.once.Do(func() {
		if e.parent.Err() != nil {
			out = workspaceExit{reason: "signal"}
		}
		e.decision <- out // exactly one send; the sole slot cannot block under a provider lock
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
	var out workspaceExit
	select {
	case code := <-finished:
		select {
		case out = <-e.decision:
			return reportWorkspaceExit(out)
		default:
			return code
		}
	case <-ctx.Done():
		e.choose(workspaceExit{reason: "signal"})
		out = <-e.decision
	case out = <-e.decision:
	}
	if out.code == 0 {
		<-finished
		return 0
	}
	timer := time.NewTimer(volume.WorkspaceDrainTimeout)
	defer timer.Stop()
	return finishWorkspaceExit(out, finished, timer.C)
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

// INV-WS-STARTUP-RETRY-001: only a newer other-replica authority fact
// permits no-retry. Host owns the lineage observation and shared predicate.
func workspaceRetryOutcome(reason string, h *volume.Host) workspaceExit {
	if h.IsCurrentReplicaSuperseded() {
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
			out = workspaceRetryOutcome("lease_expired", h)
		}
		if out.code != 0 {
			e.choose(out)
		}
		return &request, done
	case err := <-done:
		fmt.Fprintln(os.Stderr, "blockvolume: workspace lease stopped:", err)
		e.choose(workspaceRetryOutcome("lease_expired", h))
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
			e.choose(workspaceExit{reason: "revoked"})
			return
		}
		fmt.Fprintln(os.Stderr, "blockvolume: workspace lease closeout:", err)
	case <-time.After(workspaceLeaseCloseoutTimeout):
		fmt.Fprintln(os.Stderr, "blockvolume: workspace lease closeout deadline")
	}
	e.choose(workspaceExit{exitWorkspaceRetry, "revoke_ack_unconfirmed"})
}

func chooseChunkStartupExit(e *processExit, f flags, h *volume.Host, p *durable.DurableProvider) bool {
	if !f.workspaceLease || f.durableImpl != "walstore-chunk" {
		return false
	}
	gen, failed := p.ChunkStartupOutcome(f.volumeID)
	if !failed {
		return false
	}
	out := workspaceRetryOutcome("assignment_timeout", h)
	return p.ChooseChunkStartupExit(f.volumeID, gen, func() { e.choose(out) })
}
