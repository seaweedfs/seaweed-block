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
	// Post-drain ACK wait; M2 Revoking -> Revoked, Part D. It covers the
	// guard's stop reply (the resource owner has drained by then) and its
	// ACK call, so it exceeds the ACK's own bound by 1 s.
	workspaceLeaseCloseoutTimeout = volume.WorkspaceAckTimeout + time.Second
)

type workspaceExit struct {
	code   int
	reason string
	// detail is the operator's cure, printed on the final exit line.
	detail string
}

// INV-WS-STARTUP-RETRY-001: the first outcome is immutable. The channel is a
// single latched wakeup, not a queue of competing shutdown events.
type processExit struct {
	parent        context.Context
	once          sync.Once
	done          chan struct{}
	decision      chan workspaceExit
	startupMu     sync.Mutex
	startupCtx    context.Context
	startupCancel context.CancelFunc
}

func newProcessExit(ctx context.Context) *processExit {
	startupCtx, cancel := context.WithCancel(ctx)
	return &processExit{parent: ctx, done: make(chan struct{}), decision: make(chan workspaceExit, 1), startupCtx: startupCtx, startupCancel: cancel}
}

// INV-WS-STARTUP-RETRY-001: once the resource owner takes shutdown, startup
// outcomes cannot arbitrate it. ACK success is still selected only after ACK.
func (e *processExit) stopChunkStartup() {
	e.startupMu.Lock()
	defer e.startupMu.Unlock()
	e.startupCancel()
}
func (e *processExit) chooseChunkStartup(out workspaceExit) {
	e.startupMu.Lock()
	defer e.startupMu.Unlock()
	if e.startupCtx.Err() == nil {
		e.choose(out)
	}
}

func (e *processExit) choose(out workspaceExit) {
	e.once.Do(func() {
		if e.parent.Err() != nil {
			out = workspaceExit{reason: "signal"}
		}
		e.decision <- out // exactly one send fits the buffer without blocking under a provider lock
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
		if out.detail != "" {
			fmt.Fprintf(os.Stderr, "blockvolume: exiting status=%d reason=%s cure=%q\n", out.code, out.reason, out.detail)
		} else {
			fmt.Fprintf(os.Stderr, "blockvolume: exiting status=%d reason=%s\n", out.code, out.reason)
		}
	}
	return out.code
}

// INV-WS-STARTUP-RETRY-001: one decision combines a newer other-replica
// authority fact and the provider's generation-bound foreign-HEAD proof.
func workspaceRetryOutcome(reason string, h *volume.Host, claimSuperseded bool) workspaceExit {
	if claimSuperseded || h.IsCurrentReplicaSuperseded() {
		return workspaceExit{code: exitWorkspaceSuperseded, reason: "superseded"}
	}
	return workspaceExit{code: exitWorkspaceRetry, reason: reason}
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
		case <-e.startupCtx.Done():
			return
		case <-ctx.Done():
			return
		case <-e.done:
			return
		case <-p.ChunkStartupUpdates():
		}
	}
}

// chunkRuntimeFenceProvider reports a runtime HEAD fence of the attached layer.
type chunkRuntimeFenceProvider interface {
	ChunkRuntimeFenced(string) bool
	ChunkRuntimeFenceUpdates() <-chan struct{}
}

// watchChunkRuntimeFence: a HEAD fence after startup, while the lease may
// still be valid, ends the process with no restart (78 head_fenced): another
// holder owns HEAD (INV-FE-PRIMARY-READY-OPERATIONAL-001). Workspace chunk
// primaries only.
func watchChunkRuntimeFence(ctx context.Context, e *processExit, f flags, p chunkRuntimeFenceProvider) {
	if !f.workspaceLease || f.durableImpl != "walstore-chunk" {
		return
	}
	for {
		if p.ChunkRuntimeFenced(f.volumeID) {
			e.choose(workspaceExit{code: exitWorkspaceSuperseded, reason: "head_fenced"})
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-e.done:
			return
		case <-p.ChunkRuntimeFenceUpdates():
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
		fmt.Fprintln(os.Stderr, "blockvolume: stopping: the workspace lease guard fenced and requested the drain")
		e.stopChunkStartup()
		out := workspaceExit{reason: "revoked"}
		if !h.WorkspaceLeaseRevoking() {
			out = workspaceRetryOutcome("lease_expired", h, false)
		}
		if out.code != 0 {
			e.choose(out)
		}
		return &request, done
	case err := <-done:
		e.stopChunkStartup()
		fmt.Fprintln(os.Stderr, "blockvolume: workspace lease stopped:", err)
		e.choose(workspaceRetryOutcome("lease_expired", h, false))
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
	e.choose(workspaceExit{code: exitWorkspaceRetry, reason: "revoke_ack_unconfirmed"})
}

// The provider publishes only a generation-bound outcome and an atomic choice.
type chunkStartupOutcomeProvider interface {
	ChunkStartupOutcome(string) (uint64, bool)
	ChunkStartupSuperseded(string, uint64) bool
	ChooseChunkStartupExit(string, uint64, func()) bool
}

func chooseChunkStartupExit(e *processExit, f flags, h *volume.Host, p chunkStartupOutcomeProvider) bool {
	if !f.workspaceLease || f.durableImpl != "walstore-chunk" {
		return false
	}
	gen, failed := p.ChunkStartupOutcome(f.volumeID)
	if !failed {
		return false
	}
	// INV-WS-STARTUP-RETRY-001: one exit decision consumes both proof
	// sources; the provider rechecks the claim generation below.
	out := workspaceRetryOutcome("assignment_timeout", h, p.ChunkStartupSuperseded(f.volumeID, gen))
	if identity, ok := p.(interface{ ChunkStartupIdentityRefused(string, uint64) bool }); ok && identity.ChunkStartupIdentityRefused(f.volumeID, gen) {
		out = workspaceExit{code: exitWorkspaceSuperseded, reason: "layout_identity_mismatch", detail: "VOLUME/HEAD disagrees with --layout-id; retry with the layout id from the master's volume record"}
	}
	if h != nil {
		// A typed MOVED or DELETED master refusal is permanent: 78 at once,
		// never a restart loop to the unit's start limit. It is the reason
		// that ended the wait, so it names the exit even when a newer
		// foreign line was also observed (superseded).
		if _, final := h.MasterWaitState(); final != "" {
			out = workspaceExit{code: exitWorkspaceSuperseded, reason: final, detail: h.MasterWaitDetail()}
		}
	}
	if c, ok := p.(interface{ ChunkStartupCure(string, uint64) string }); ok {
		if cure := c.ChunkStartupCure(f.volumeID, gen); cure != "" {
			out.detail = cure
		}
	}
	return p.ChooseChunkStartupExit(f.volumeID, gen, func() { e.chooseChunkStartup(out) })
}
