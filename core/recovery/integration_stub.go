package recovery

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// integration_stub.go is a *thin* integration shim that demonstrates
// how the recovery package would be called from the existing
// `core/transport/BlockExecutor` and `core/adapter/CommandExecutor`
// surfaces, WITHOUT replacing them. Production integration will live
// in those packages — this file proves the lifecycle/callback shape
// so mismatches surface early (per architect's priority #1).
//
// Per architect ruling on G7-redo Layer-2 review: "与 BlockExecutor /
// engine 出一条「薄」链路 — 单次 Run(session) 可调、不换全 transport".
//
// P2c-slice B-2 lifecycle change: barrier auto-fires when sink.DrainBacklog
// returns (the bridging sink flushes its live-write buffer + seals there).
// Engine no longer needs an explicit "go barrier" signal — the previous
// FinishLiveWrites / sender.Close pattern is gone. Live writes that arrive
// after the sink seals get a "session is closing" error from PushLiveWrite
// so callers can fall back to steady-live or retry on a fresh session.
//
// What this file deliberately does NOT do:
//
//   - Plug into core/transport/BlockExecutor.StartRebuild (that is
//     the integration PR's job).
//   - Replace the existing wire format (we use this package's frame
//     format on a separate connection; production may keep both
//     paths in parallel behind a flag).
//   - Implement the full CommandExecutor surface (Probe, Fence,
//     CatchUp, etc.); this stub covers only the rebuild lifecycle.

// SessionStartCallback fires once per session AFTER the wire
// connection is established and the SessionStart frame has been
// written. Mirrors the existing `OnSessionStart` callback shape.
type SessionStartCallback func(replicaID ReplicaID, sessionID uint64)

// SessionCloseCallback fires once per session at termination
// (success or failure). Mirrors the existing `OnSessionClose`
// callback shape: success means achievedLSN ≥ targetLSN; on failure
// `err` is non-nil and `achievedLSN` is 0.
type SessionCloseCallback func(replicaID ReplicaID, sessionID uint64, achievedLSN uint64, err error)

// PrimaryBridge is the primary-side adapter. One bridge instance
// per primary daemon; it serializes session starts per replica via
// the embedded coordinator's INV-SINGLE-FLIGHT-PER-REPLICA check.
//
// Live writes for active sessions are pushed via PushLiveWrite,
// which the WAL shipper calls after consulting RouteLocalWrite. The
// session converges autonomously: sender.Run barriers as soon as
// sink.DrainBacklog returns (P2c-slice B-2 lifecycle).
type PrimaryBridge struct {
	store storage.LogicalStorage
	coord *PeerShipCoordinator

	onStart SessionStartCallback
	onClose SessionCloseCallback

	mu      sync.Mutex
	senders map[ReplicaID]*Sender
}

// NewPrimaryBridge constructs a bridge bound to the primary's
// substrate. Callbacks may be nil (fire-and-forget).
func NewPrimaryBridge(store storage.LogicalStorage, coord *PeerShipCoordinator, onStart SessionStartCallback, onClose SessionCloseCallback) *PrimaryBridge {
	return &PrimaryBridge{
		store:   store,
		coord:   coord,
		onStart: onStart,
		onClose: onClose,
		senders: make(map[ReplicaID]*Sender),
	}
}

// StartRebuildSession is shaped like CommandExecutor.StartRebuild:
// non-blocking, returns immediately after spawning the session
// goroutine. After P2c-slice B-2 the session terminates on its own
// when sink.DrainBacklog returns; the caller can still abort early
// via context cancellation, but no explicit "go barrier" call is
// needed.
//
// This method uses the in-package `senderBacklogSink` (bridging path,
// emits frameWALEntry on the dual-lane port). Callers that want to
// route the recovery WAL pump through a real `transport.WalShipper`
// adapter use `StartRebuildSessionWithSink` instead — the wire format
// alignment between the two paths is gated by P2d.
//
// Wire detail (POC): caller provides the dialed `conn`. Production
// integration would dial through the existing connection pool /
// listener registry.
func (b *PrimaryBridge) StartRebuildSession(ctx context.Context, conn net.Conn, replicaID ReplicaID, sessionID, fromLSN, targetLSN uint64) error {
	return b.startRebuildSessionLocked(ctx, conn, replicaID, sessionID, fromLSN, targetLSN, nil)
}

// StartRebuildSessionWithSink is the variant that accepts an external
// `WalShipperSink` — typically a `transport.RecoverySink` that wraps
// the per-replica `WalShipper` and enforces architect rules 1+2
// (emit context set BEFORE StartSession; restored AFTER EndSession).
//
// Pre-decision parallel-safe wiring: this method exists so production
// callers can plug a real adapter today without changing the bridge's
// public surface later. The actual format compatibility between the
// adapter's emits and the receiver's decoder is gated by P2d — until
// that decision lands, callers should typically prefer the bridging
// `StartRebuildSession` to avoid wire-format mismatch in production.
//
// `sink` MUST be non-nil; nil reverts to the bridging path (use
// `StartRebuildSession` for that explicitly).
func (b *PrimaryBridge) StartRebuildSessionWithSink(ctx context.Context, conn net.Conn, replicaID ReplicaID, sessionID, fromLSN, targetLSN uint64, sink WalShipperSink) error {
	if sink == nil {
		return fmt.Errorf("recovery bridge: StartRebuildSessionWithSink: sink is required (use StartRebuildSession for the bridging path)")
	}
	return b.startRebuildSessionLocked(ctx, conn, replicaID, sessionID, fromLSN, targetLSN, sink)
}

// startRebuildSessionLocked is the shared implementation. When `sink`
// is nil, it constructs a bridging `senderBacklogSink` via
// NewSenderWithBacklogRelay (legacy compatible). When non-nil, the
// caller-provided sink is installed via NewSenderWithSink.
func (b *PrimaryBridge) startRebuildSessionLocked(ctx context.Context, conn net.Conn, replicaID ReplicaID, sessionID, fromLSN, targetLSN uint64, sink WalShipperSink) error {
	// Single-flight check at the bridge level (before coordinator),
	// so a second call observes our internal sender map first and
	// reports the racing-bridge-call case distinctly from the
	// coordinator's INV-SINGLE-FLIGHT-PER-REPLICA.
	b.mu.Lock()
	if _, busy := b.senders[replicaID]; busy {
		b.mu.Unlock()
		return fmt.Errorf("recovery bridge: replica %q already has active sender (single-flight)", replicaID)
	}

	// Register the session with the coordinator SYNCHRONOUSLY so the
	// caller's next read of `coord.RouteLocalWrite` deterministically
	// observes a non-Idle phase. If StartSession fails we don't
	// install the sender or fire OnStart.
	if err := b.coord.StartSession(replicaID, sessionID, fromLSN, targetLSN); err != nil {
		b.mu.Unlock()
		return fmt.Errorf("recovery bridge: %w", err)
	}

	var sender *Sender
	if sink == nil {
		sender = NewSenderWithBacklogRelay(b.store, b.coord, conn, replicaID)
	} else {
		sender = NewSenderWithSink(b.store, b.coord, conn, replicaID, sink)
	}
	b.senders[replicaID] = sender
	b.mu.Unlock()

	if b.onStart != nil {
		b.onStart(replicaID, sessionID)
	}

	go func() {
		achieved, err := sender.Run(ctx, sessionID, fromLSN, targetLSN)
		b.mu.Lock()
		delete(b.senders, replicaID)
		b.mu.Unlock()
		if b.onClose != nil {
			b.onClose(replicaID, sessionID, achieved, err)
		}
	}()
	return nil
}

// PushLiveWrite forwards a primary-side WAL append to the active
// session for this peer (if any). Caller is the WAL shipper
// integration; they MUST have already consulted
// `coord.RouteLocalWrite(replicaID, lsn)` and confirmed
// RouteSessionLane.
//
// Returns nil if the entry was queued, or error if no session is
// active or the session has sealed for barrier.
func (b *PrimaryBridge) PushLiveWrite(replicaID ReplicaID, lba uint32, lsn uint64, data []byte) error {
	b.mu.Lock()
	sender, ok := b.senders[replicaID]
	b.mu.Unlock()
	if !ok {
		return fmt.Errorf("recovery bridge: no active session for replica %q", replicaID)
	}
	return sender.PushLiveWrite(lba, lsn, data)
}

// ReplicaBridge is the replica-side adapter. One bridge instance per
// replica daemon; it accepts inbound recover-session connections and
// drives a Receiver per connection.
//
// Production integration: this would be a side-channel listener on
// a separate port from the existing ReplicaListener (parallel-path,
// flag-toggled, per architect ruling).
type ReplicaBridge struct {
	store storage.LogicalStorage
}

// NewReplicaBridge constructs a bridge bound to the replica's substrate.
func NewReplicaBridge(store storage.LogicalStorage) *ReplicaBridge {
	return &ReplicaBridge{store: store}
}

// Serve accepts one inbound connection and drives a Receiver until
// barrier ack returns or the connection errors. Returns the
// achievedLSN reported on the wire, or an error.
//
// Caller spawns this in a goroutine per accepted connection.
func (b *ReplicaBridge) Serve(ctx context.Context, conn net.Conn) (uint64, error) {
	receiver := NewReceiver(b.store, conn)
	// Caller is responsible for closing conn on return; wire the
	// ctx through a goroutine that closes the conn on cancel so
	// receiver.Run returns from its blocking read.
	if ctx != nil {
		go func() {
			<-ctx.Done()
			_ = conn.Close()
		}()
	}
	return receiver.Run()
}

// AcceptDualLaneLoop runs an accept loop on the given listener,
// dispatching every accepted connection to a fresh ReplicaBridge.Serve
// goroutine. Returns when the listener is closed (caller's signal to
// stop is `ln.Close()`). Production daemon (cmd/blockvolume) uses this
// to bind the dual-lane port; tests can use it directly with a
// temporary `net.Listen("tcp", "127.0.0.1:0")`.
//
// The function is non-blocking-safe: each conn handler runs in its
// own goroutine, so a long-lived recovery session does not block
// further accepts.
//
// Lifecycle: this loop OWNS the goroutines it spawns until the
// listener is closed. After ln.Close, in-flight handlers continue
// (they exit on ctx cancel or their own conn read error). Caller
// owns ctx and listener lifecycles.
func (b *ReplicaBridge) AcceptDualLaneLoop(ctx context.Context, ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			// Listener closed (caller's signal) or fatal accept error;
			// either way, stop. Per-conn goroutines spawned earlier
			// keep running until they exit on their own.
			return
		}
		go func(c net.Conn) {
			subCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			_, _ = b.Serve(subCtx, c)
		}(conn)
	}
}
