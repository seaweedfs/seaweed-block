package transport

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/recovery"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// BlockExecutor implements adapter.CommandExecutor using real TCP
// transport and a primary-side LogicalStorage. It is the "muscle"
// layer — it executes commands but never decides policy.
//
// Recovery mode: nil DualLane => legacy single-lane path (rebuild
// streams MsgRebuildBlock and rejects live ship during rebuild).
// non-nil DualLane => StartRebuild delegates to recovery package's
// PrimaryBridge, dialing the replica's separate dual-lane port (per
// docs/recovery-wiring-plan.md §4 Option A — separate ports).
// Architect ACK on parallel-flag strategy (2026-04-29).
type BlockExecutor struct {
	primaryStore storage.LogicalStorage
	replicaAddr  string // replica's legacy port (rebuild + ship + barrier + probe)

	// dualLane is non-nil when this executor is configured for
	// dual-lane recovery. StartRebuild branches on this; ship +
	// probe + fence always use the legacy path on replicaAddr.
	dualLane *DualLaneConfig

	mu              sync.Mutex
	onSessionStart  adapter.OnSessionStart
	onSessionClose  adapter.OnSessionClose
	onFenceComplete adapter.OnFenceComplete
	sessions        map[uint64]*activeSession
	stepDelay       time.Duration

	// walShippers is the per-replica WalShipper registry per
	// v3-recovery-wal-shipper-spec.md §3 INV-SINGLE: at most one
	// WalShipper per (volume, replicaID). Get-or-create via
	// WalShipperFor. Each entry owns a shipper + a mutable emit-conn
	// pointer that Ship() updates before delegating NotifyAppend.
	walShipperMu sync.Mutex
	walShippers  map[string]*walShipperEntry
}

// EmitProfile selects the wire encoder + frame format used by the
// per-replica WalShipper's EmitFunc. P2d (architect 2026-04-30):
// dual-lane connections use frameWALEntry; legacy steady connections
// use MsgShipEntry. The profile is part of the emit context (alongside
// conn + lineage) and switches when recovery sessions bracket the
// shipper into the dual-lane port.
type EmitProfile int

const (
	// EmitProfileSteadyMsgShip — legacy port WAL ship: SWRP envelope
	// (`WriteMsg(MsgShipEntry, EncodeShipEntry(...))`). Used outside
	// recovery sessions and by Ship() in steady state.
	EmitProfileSteadyMsgShip EmitProfile = iota
	// EmitProfileDualLaneWALFrame — recovery dual-lane port: 5-byte
	// recovery frame header + encodeWALEntry payload (via
	// recovery.WriteWALEntryFrame). Used inside recovery sessions on
	// the dual-lane connection. Kind tag derives from the EmitKind
	// passed by WalShipper: Backlog or SessionLive.
	EmitProfileDualLaneWALFrame
)

func (p EmitProfile) String() string {
	switch p {
	case EmitProfileSteadyMsgShip:
		return "SteadyMsgShip"
	case EmitProfileDualLaneWALFrame:
		return "DualLaneWALFrame"
	default:
		return fmt.Sprintf("EmitProfile(%d)", int(p))
	}
}

// walShipperEntry pairs a per-replica WalShipper with its mutable
// wire-emit context (conn + lineage + profile). Ship() / recovery-
// session initialization updates the context under emitCtxMu before
// delegating to the shipper; the EmitFunc reads it under the same
// mutex.
//
// Architect P1 review (2026-04-29 #1): encoding MUST happen INSIDE
// the EmitFunc, not by the caller before NotifyAppend. WalShipper
// hands `(kind, lba, lsn, raw block bytes)` to EmitFunc; EmitFunc
// owns the encoding so steady Ship() AND recovery-session emits
// feed the SAME EmitFunc, with the profile selecting which wire
// frame format to emit.
//
// emitLineage is the lineage embedded in MsgShipEntry payloads
// (steady profile only — frameWALEntry has no lineage field).
// For steady-state ship, Ship() updates it from the active session
// per call. For recovery-session emits, the session-installer
// (RecoverySink) updates context at StartSession and restores
// steady context at EndSession.
type walShipperEntry struct {
	shipper *WalShipper

	emitCtxMu   sync.Mutex
	emitConn    net.Conn
	emitLineage RecoveryLineage
	emitProfile EmitProfile
	// postEmit fires after a successful conn.Write; recovery sessions
	// install a callback that calls coord.RecordShipped(replicaID, lsn)
	// so shipCursor advances atomically with emit. nil = no-op.
	postEmit func(lsn uint64)

	// writeMu serializes conn.Write on the entry's emitConn across
	// EmitFunc and any external writer that shares this conn (notably
	// recovery.Sender's writeFrame during a dual-lane session). Without
	// this shared mutex, concurrent header+payload writes from the two
	// paths can interleave and punch through frame boundaries.
	//
	// In steady state (Ship → only one writer per legacy conn), the
	// mutex is uncontended. In dual-lane sessions, recovery.Sender
	// duck-types `WriteMu() *sync.Mutex` on the sink and uses this
	// mutex for its own writeFrame calls.
	//
	// Lock hierarchy (architect §6.8 mechanical SINGLE-SERIALIZER):
	//
	//   walShipper.shipMu  ───>  walShipperEntry.writeMu
	//
	//   - WalShipper holds shipMu around mode/cursor mutations and
	//     calls EmitFunc under it (NotifyAppend Realtime, DrainBacklog
	//     scan callback, drainOpportunity scan callback). EmitFunc
	//     then acquires writeMu for the wire write — shipMu is
	//     OUTERMOST.
	//   - recovery.Sender.writeFrame acquires only writeMu (Sender
	//     does not hold shipMu).
	//
	// Therefore: any code path that holds shipMu may acquire writeMu,
	// but NEVER the reverse. Reversing this order risks deadlock.
	writeMu sync.Mutex
}

// DualLaneConfig holds the wiring for routing StartRebuild through
// the core/recovery dual-lane mechanism. Constructed at cmd / daemon
// startup when --recovery-mode=dual-lane.
type DualLaneConfig struct {
	// Bridge is the primary-side adapter built once per executor
	// (bridge captures executor's callback redirections via closures
	// in NewBlockExecutorWithDualLane).
	Bridge *recovery.PrimaryBridge
	// DialAddr is the replica's dual-lane listen address (separate
	// port from replicaAddr per the wiring plan §4 Option A).
	DialAddr string
	// ReplicaID identifies this replica in the coordinator's
	// per-peer state. Must match the receiver's session view.
	ReplicaID recovery.ReplicaID
}

type activeSession struct {
	lineage RecoveryLineage
	cancel  chan struct{}
	conn    net.Conn
}

var errSessionInvalidated = errors.New("session invalidated")

const recoveryConnTimeout = 5 * time.Second

// NewBlockExecutor creates a legacy-mode executor for one primary -> replica
// pair. StartRebuild uses the existing single-lane MsgRebuildBlock path.
func NewBlockExecutor(primaryStore storage.LogicalStorage, replicaAddr string) *BlockExecutor {
	return &BlockExecutor{
		primaryStore: primaryStore,
		replicaAddr:  replicaAddr,
		sessions:     make(map[uint64]*activeSession),
	}
}

// NewBlockExecutorWithDualLane creates a dual-lane-mode executor.
// StartRebuild routes through the recovery package's PrimaryBridge,
// dialing dualLaneAddr (the replica's separate listen port). All other
// methods (Probe / Fence / StartCatchUp / ship via Ship()) continue to
// use replicaAddr's legacy port.
//
// `coord` must be a per-volume coordinator shared across all replicas
// of the same volume so MinPinAcrossActiveSessions reflects the true
// minimum (per wiring plan §5).
//
// `replicaID` MUST match the SessionStart payload the replica decodes;
// in production it is the engine's per-peer identifier as a string.
//
// The bridge captures executor-state closures so that subsequent
// SetOnSessionStart / SetOnSessionClose calls on this executor
// propagate to bridge callbacks dynamically.
func NewBlockExecutorWithDualLane(
	primaryStore storage.LogicalStorage,
	replicaAddr string,
	dualLaneAddr string,
	coord *recovery.PeerShipCoordinator,
	replicaID recovery.ReplicaID,
) *BlockExecutor {
	e := NewBlockExecutor(primaryStore, replicaAddr)
	bridge := recovery.NewPrimaryBridge(
		primaryStore,
		coord,
		func(rid recovery.ReplicaID, sid uint64) {
			e.mu.Lock()
			cb := e.onSessionStart
			e.mu.Unlock()
			if cb != nil {
				cb(adapter.SessionStartResult{SessionID: sid})
			}
		},
		func(rid recovery.ReplicaID, sid uint64, achieved uint64, err error) {
			e.mu.Lock()
			cb := e.onSessionClose
			e.mu.Unlock()
			if cb == nil {
				return
			}
			res := adapter.SessionCloseResult{
				SessionID:   sid,
				AchievedLSN: achieved,
			}
			if err == nil {
				res.Success = true
			} else {
				res.FailReason = err.Error()
			}
			cb(res)
		},
	)
	e.dualLane = &DualLaneConfig{
		Bridge:    bridge,
		DialAddr:  dualLaneAddr,
		ReplicaID: replicaID,
	}
	return e
}

func (e *BlockExecutor) SetOnSessionClose(fn adapter.OnSessionClose) {
	e.onSessionClose = fn
}

func (e *BlockExecutor) SetOnSessionStart(fn adapter.OnSessionStart) {
	e.onSessionStart = fn
}

func (e *BlockExecutor) SetStepDelay(d time.Duration) {
	e.mu.Lock()
	e.stepDelay = d
	e.mu.Unlock()
}

func (e *BlockExecutor) SetOnFenceComplete(fn adapter.OnFenceComplete) {
	e.onFenceComplete = fn
}

// Fence sends one MsgBarrierReq at the given lineage to the
// replica and awaits MsgBarrierResp. Does NOT ship any blocks and
// does NOT go through the session registry — fence is a single
// barrier exchange, not a recovery session.
//
// On success or failure, fires the registered OnFenceComplete
// callback exactly once. Retry is the adapter/engine's
// responsibility (probe-driven); this method never retries.
func (e *BlockExecutor) Fence(replicaID string, sessionID, epoch, endpointVersion uint64) error {
	lineage := RecoveryLineage{
		SessionID:       sessionID,
		Epoch:           epoch,
		EndpointVersion: endpointVersion,
		// TargetLSN is not semantically meaningful for fence (fence
		// doesn't declare a recovery target), but post-T4b-1 the
		// barrier-ack wire demands every lineage field be non-zero
		// (architect round-21 uniform rule applied at decode). Using
		// the liveShipTargetLSN sentinel (1) keeps Fence composable
		// with the stricter wire while preserving the "not a recovery
		// target" intent — no TargetLSN-sensitive code in the fence
		// path reads this value.
		TargetLSN: fenceSentinelTargetLSN,
	}
	go e.doFence(replicaID, lineage)
	return nil
}

// fenceSentinelTargetLSN is the lineage.TargetLSN value used for
// fence requests. Arbitrary non-zero value; matches the
// liveShipTargetLSN convention on the peer-level live ship path.
// See Fence() godoc for why this is non-zero.
const fenceSentinelTargetLSN uint64 = 1

// FenceSync performs a synchronous fence exchange against the replica:
// dial a fresh conn, send MsgBarrierReq at the given lineage, read
// MsgBarrierResp, validate the full-lineage echo, and return. Returns
// nil on success or a descriptive error on any dial / send / recv /
// lineage-mismatch failure.
//
// Does NOT go through the session registry — fence is a single barrier
// exchange, not a recovery session. Does NOT fire OnFenceComplete; the
// async entry point Fence() wraps FenceSync and handles the callback.
//
// Called by: BlockExecutor.doFence (async wrapper for legacy callback-
// driven callers like the engine's recovery path) and
// ReplicaPeer.Fence (T4b-3 wrapper for sync-style callers like
// DurabilityCoordinator).
// Owns: per-call conn dial + deadline; lineage validation against the
// request lineage (round-21 uniform rule).
// Borrows: lineage from caller.
func (e *BlockExecutor) FenceSync(replicaID string, lineage RecoveryLineage) error {
	conn, err := net.DialTimeout("tcp", e.replicaAddr, 2*time.Second)
	if err != nil {
		return fmt.Errorf("fence dial: %w", err)
	}
	defer conn.Close()

	if err := sendBarrierReq(conn, lineage, recoveryConnTimeout); err != nil {
		return fmt.Errorf("fence barrier send: %w", err)
	}
	resp, err := recvBarrierResp(conn, recoveryConnTimeout)
	if err != nil {
		// Short / zero-valued / field-order-malformed payloads are
		// rejected at the T4b-1 decode layer as errors; they surface
		// here without silent-accept (architect round 22 uniform rule
		// at the first ack-consuming surface).
		return fmt.Errorf("fence barrier resp: %w", err)
	}
	// T4b-2 round-22 lineage validation: the replica's echoed lineage
	// MUST match the lineage we sent.
	if resp.Lineage != lineage {
		log.Printf("executor: fence lineage mismatch replica=%s expected=%+v actual=%+v",
			replicaID, lineage, resp.Lineage)
		return fmt.Errorf("fence barrier resp lineage mismatch: expected=%+v actual=%+v",
			lineage, resp.Lineage)
	}
	log.Printf("executor: fence complete replica=%s epoch=%d", replicaID, lineage.Epoch)
	return nil
}

func (e *BlockExecutor) doFence(replicaID string, lineage RecoveryLineage) {
	result := adapter.FenceResult{
		ReplicaID:       replicaID,
		SessionID:       lineage.SessionID,
		Epoch:           lineage.Epoch,
		EndpointVersion: lineage.EndpointVersion,
	}
	if err := e.FenceSync(replicaID, lineage); err != nil {
		result.Success = false
		result.FailReason = err.Error()
	} else {
		result.Success = true
	}
	e.fireFenceComplete(result)
}

// Barrier issues a per-peer barrier round-trip carrying the session
// lineage. Blocks until the response arrives or the deadline fires.
// Returns the validated BarrierAck on success, or error on transport
// failure / ack rejection.
//
// Architect round-21 uniform rule applies end-to-end:
//   - decode rejects short / zeroed / field-order-malformed acks
//     (T4b-1 wire)
//   - this method rejects valid-decode-but-wrong-session acks via
//     ErrBarrierLineageMismatch (T4b-2 ack-consumer validation)
// A future optimization MUST NOT weaken this to epoch-only; the
// full-lineage rule is load-bearing for H5 LOCK.
//
// Called by: DurabilityCoordinator.SyncLocalAndReplicas (T4b-3)
// per-peer fan-out.
// Owns: per-call conn deadline (recoveryConnTimeout); lineage binding;
// ack validation against the session's registered lineage.
// Borrows: session by replicaID + lineage.SessionID; BarrierAck
// payload fields are value-copied into the returned struct.
func (e *BlockExecutor) Barrier(replicaID string, lineage RecoveryLineage, targetLSN uint64) (BarrierAck, error) {
	// Session lookup + epoch-== fence (same pattern as Ship).
	e.mu.Lock()
	session, ok := e.sessions[lineage.SessionID]
	if !ok || session == nil {
		e.mu.Unlock()
		return BarrierAck{}, fmt.Errorf("transport: barrier: no session for replica=%s sessionID=%d",
			replicaID, lineage.SessionID)
	}
	if lineage.Epoch != session.lineage.Epoch {
		sessionEpoch := session.lineage.Epoch
		e.mu.Unlock()
		log.Printf("transport: barrier: dropping targetLSN=%d replica=%s stale epoch %d (session epoch %d)",
			targetLSN, replicaID, lineage.Epoch, sessionEpoch)
		return BarrierAck{}, fmt.Errorf("transport: barrier: stale epoch %d (session %d)",
			lineage.Epoch, sessionEpoch)
	}
	conn := session.conn
	e.mu.Unlock()

	// Lazy-dial if no conn (same pattern as Ship, architect round-11
	// Option B). Attach-under-mu with "first winner keeps" races.
	if conn == nil {
		dialed, err := net.DialTimeout("tcp", e.replicaAddr, shipDialTimeout)
		if err != nil {
			return BarrierAck{}, fmt.Errorf("transport: barrier: dial replica=%s sessionID=%d: %w",
				replicaID, lineage.SessionID, err)
		}
		e.mu.Lock()
		current, stillActive := e.sessions[lineage.SessionID]
		switch {
		case !stillActive || current != session:
			e.mu.Unlock()
			_ = dialed.Close()
			return BarrierAck{}, fmt.Errorf("transport: barrier: session %d invalidated during dial",
				lineage.SessionID)
		case session.conn != nil:
			conn = session.conn
			e.mu.Unlock()
			_ = dialed.Close()
		default:
			session.conn = dialed
			conn = dialed
			e.mu.Unlock()
		}
	}

	// Send request + receive response with deadlines on both sides
	// (sendBarrierReq / recvBarrierResp already set conn deadline).
	if err := sendBarrierReq(conn, lineage, recoveryConnTimeout); err != nil {
		return BarrierAck{}, fmt.Errorf("transport: barrier: send replica=%s: %w", replicaID, err)
	}
	resp, err := recvBarrierResp(conn, recoveryConnTimeout)
	if err != nil {
		return BarrierAck{}, fmt.Errorf("transport: barrier: recv replica=%s: %w", replicaID, err)
	}

	// Full-lineage validation (architect round-21 uniform rule).
	// Stale or cross-session acks fail here. Diagnostic log format
	// matches architect's round-21 text: peer ID + full expected /
	// actual lineage tuple.
	if resp.Lineage != lineage {
		log.Printf("transport: barrier: lineage mismatch replica=%s expected=%+v actual=%+v",
			replicaID, lineage, resp.Lineage)
		return BarrierAck{}, ErrBarrierLineageMismatch
	}

	return BarrierAck{
		Lineage:     resp.Lineage,
		AchievedLSN: resp.AchievedLSN,
		Success:     true,
	}, nil
}

func (e *BlockExecutor) fireFenceComplete(result adapter.FenceResult) {
	e.mu.Lock()
	cb := e.onFenceComplete
	e.mu.Unlock()
	if cb != nil {
		cb(result)
	}
}

// ErrProbeLineageMismatch is returned by `BlockExecutor.Probe` when the
// replica's response carries a lineage that does not match the request
// lineage. Per architect round-21 + round-26 symmetric-pair rule, such
// responses MUST NOT contribute to recovery facts — the executor surfaces
// the error verbatim and the engine treats it as a probe failure.
var ErrProbeLineageMismatch = errors.New("transport: probe: response lineage does not match request")

// Probe dials the replica, sends a `MsgProbeReq` carrying the full
// transient probe lineage (T4c-1 wire), and returns the replica's R/S/H
// boundaries after validating the echoed lineage. Returns facts only —
// never decides policy.
//
// Per T4c-1 (architect Option D): sessionID is the adapter-minted
// transient probe sessionID. It is NOT registered in `executor.sessions`,
// NOT carried in any session-lifecycle event. The lineage built here:
//
//	RecoveryLineage{SessionID, Epoch, EndpointVersion, TargetLSN: max(1, primaryH)}
//
// is consumed only by the wire pair. `primaryH` comes from
// `primaryStore.Boundaries()`; the `max(1, ...)` floor ensures
// architect's no-zero-TargetLSN rule holds even on an empty primary.
//
// The replica's `acceptMutationLineage` accepts a fresh higher-tuple
// lineage normally — probe lineages monotonically advance with the
// minted sessionID counter, so they don't masquerade as stale.
//
// Echo validation: if `resp.Lineage != requestLineage`, returns
// `ErrProbeLineageMismatch`. Catches stale / cross-session / partially-
// zeroed responses (the latter already caught at decode, but a valid-
// decode-but-wrong-session ack is the load-bearing case here).
//
// Called by: adapter.executeCommand at engine.ProbeReplica dispatch.
// Owns: dial timeout (2s); per-call conn deadline (3s); transient
// probe lineage construction.
// Borrows: primaryStore (boundaries snapshot for TargetLSN floor and
// for R/S/H facts).
func (e *BlockExecutor) Probe(replicaID, dataAddr, ctrlAddr string, sessionID, epoch, endpointVersion uint64) adapter.ProbeResult {
	addr := e.replicaAddr
	if dataAddr != "" {
		addr = dataAddr
	}

	// Get primary's boundaries before dialing — the head is needed to
	// build a non-zero TargetLSN for the request lineage, and the
	// primaryS / primaryH are also returned in the ProbeResult.
	_, primaryS, primaryH := e.primaryStore.Boundaries()

	// Probe lineage construction (architect Option D). Floor TargetLSN
	// at 1 to honor architect's no-zero-TargetLSN rule even when the
	// primary has not yet written any data.
	targetLSN := primaryH
	if targetLSN == 0 {
		targetLSN = 1
	}
	requestLineage := RecoveryLineage{
		SessionID:       sessionID,
		Epoch:           epoch,
		EndpointVersion: endpointVersion,
		TargetLSN:       targetLSN,
	}
	if requestLineage.SessionID == 0 || requestLineage.Epoch == 0 ||
		requestLineage.EndpointVersion == 0 {
		// Fail-closed: adapter must mint a non-zero sessionID; engine
		// must populate non-zero epoch + endpointVersion before
		// emitting ProbeReplica. A zero here is a programmer error.
		return adapter.ProbeResult{
			ReplicaID:       replicaID,
			Success:         false,
			EndpointVersion: endpointVersion,
			TransportEpoch:  epoch,
			FailReason:      fmt.Sprintf("probe lineage has zero field: %+v", requestLineage),
		}
	}

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return adapter.ProbeResult{
			ReplicaID:       replicaID,
			Success:         false,
			EndpointVersion: endpointVersion,
			TransportEpoch:  epoch,
			FailReason:      fmt.Sprintf("dial: %v", err),
		}
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))

	if err := WriteMsg(conn, MsgProbeReq, EncodeProbeReq(ProbeRequest{Lineage: requestLineage})); err != nil {
		return adapter.ProbeResult{
			ReplicaID:       replicaID,
			Success:         false,
			EndpointVersion: endpointVersion,
			TransportEpoch:  epoch,
			FailReason:      fmt.Sprintf("send probe: %v", err),
		}
	}

	msgType, payload, err := ReadMsg(conn)
	if err != nil || msgType != MsgProbeResp {
		return adapter.ProbeResult{
			ReplicaID:       replicaID,
			Success:         false,
			EndpointVersion: endpointVersion,
			TransportEpoch:  epoch,
			FailReason:      fmt.Sprintf("read probe resp: %v", err),
		}
	}

	resp, err := DecodeProbeResp(payload)
	if err != nil {
		return adapter.ProbeResult{
			ReplicaID:       replicaID,
			Success:         false,
			EndpointVersion: endpointVersion,
			TransportEpoch:  epoch,
			FailReason:      fmt.Sprintf("decode probe: %v", err),
		}
	}

	// Echo validation (round-26 symmetric-pair rule, mirrors T4b-2's
	// barrier echo check). Primary-side consumer MUST reject stale /
	// cross-session / partial-zero responses by full lineage tuple.
	if resp.Lineage != requestLineage {
		log.Printf("transport: probe: lineage mismatch replica=%s expected=%+v actual=%+v",
			replicaID, requestLineage, resp.Lineage)
		return adapter.ProbeResult{
			ReplicaID:       replicaID,
			Success:         false,
			EndpointVersion: endpointVersion,
			TransportEpoch:  epoch,
			FailReason:      fmt.Sprintf("%v: expected=%+v actual=%+v",
				ErrProbeLineageMismatch, requestLineage, resp.Lineage),
		}
	}

	log.Printf("executor: probe %s success R=%d S=%d H=%d",
		replicaID, resp.SyncedLSN, primaryS, primaryH)
	return adapter.ProbeResult{
		ReplicaID:         replicaID,
		Success:           true,
		EndpointVersion:   endpointVersion,
		TransportEpoch:    epoch,
		ReplicaFlushedLSN: resp.SyncedLSN, // R
		PrimaryTailLSN:    primaryS,       // S
		PrimaryHeadLSN:    primaryH,       // H
	}
}

func (e *BlockExecutor) InvalidateSession(replicaID string, sessionID uint64, reason string) {
	var conn net.Conn
	e.mu.Lock()
	session, ok := e.sessions[sessionID]
	if ok {
		delete(e.sessions, sessionID)
		conn = session.conn
		session.conn = nil
		close(session.cancel)
	}
	e.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	log.Printf("executor: invalidate session %d for %s: %s", sessionID, replicaID, reason)
}

func (e *BlockExecutor) PublishHealthy(replicaID string) {
	log.Printf("executor: publish healthy for %s", replicaID)
}

func (e *BlockExecutor) PublishDegraded(replicaID string, reason string) {
	log.Printf("executor: publish degraded for %s: %s", replicaID, reason)
}

// WalShipperFor returns the per-replica WalShipper for replicaID,
// creating one on first call. Idempotent: subsequent calls for the
// same replicaID return the same instance (INV-SINGLE per
// v3-recovery-wal-shipper-spec.md §3).
//
// The returned WalShipper is in Realtime mode immediately (Activate
// at cursor=0). Steady-state Ship() delegates here. Recovery
// sessions can later transition the same shipper into Backlog mode
// via StartSession (P2 wiring).
//
// Concurrent calls for the same replicaID linearize through
// walShipperMu; all callers observe the same instance.
func (e *BlockExecutor) WalShipperFor(replicaID string) *WalShipper {
	e.walShipperMu.Lock()
	defer e.walShipperMu.Unlock()

	if e.walShippers == nil {
		e.walShippers = make(map[string]*walShipperEntry)
	}
	if entry, ok := e.walShippers[replicaID]; ok {
		return entry.shipper
	}

	entry := &walShipperEntry{}
	emit := func(kind EmitKind, lba uint32, lsn uint64, data []byte) error {
		entry.emitCtxMu.Lock()
		conn := entry.emitConn
		lineage := entry.emitLineage
		profile := entry.emitProfile
		postEmit := entry.postEmit
		entry.emitCtxMu.Unlock()
		if conn == nil {
			// No emit context yet (Ship hasn't run; recovery session
			// hasn't installed session context). Drop silently — same
			// semantics as ModeIdle. Caller's NotifyAppend returns nil;
			// entry remains in primary substrate and will be picked up
			// by the next emit context's drain (recovery DrainBacklog
			// scan or steady catch-up). This avoids racing against
			// session-bracket installation when callers inject live
			// writes via PushLiveWrite during the WalShipperFor →
			// sink.StartSession window.
			return nil
		}

		// C1 (§6.8 #1 mechanical SINGLE-SERIALIZER): hold writeMu
		// across the wire write so concurrent recovery.Sender.writeFrame
		// (also acquiring the same mutex via WalShipperSink.WriteMu())
		// can't interleave header+payload of two frames. In steady
		// state this is uncontended (only Ship writes); in dual-lane
		// sessions it serializes Sender + WalShipper writers.
		entry.writeMu.Lock()
		_ = conn.SetWriteDeadline(time.Now().Add(shipWriteDeadline))
		var err error
		switch profile {
		case EmitProfileSteadyMsgShip:
			payload := EncodeShipEntry(ShipEntry{
				Lineage: lineage,
				LBA:     lba,
				LSN:     lsn,
				Data:    data,
			})
			err = WriteMsg(conn, MsgShipEntry, payload)
		case EmitProfileDualLaneWALFrame:
			// Map EmitKind → recovery WALEntryKind. Backlog scan emits
			// → WALKindBacklog; Realtime / NotifyAppend emits during
			// session → WALKindSessionLive (the kind tag drives the
			// receiver's BacklogApplied vs SessionLiveApplied counters
			// — observability only; apply behavior identical).
			var walKind recovery.WALEntryKind
			if kind == EmitKindBacklog {
				walKind = recovery.WALKindBacklog
			} else {
				walKind = recovery.WALKindSessionLive
			}
			err = recovery.WriteWALEntryFrame(conn, walKind, lba, lsn, data)
		default:
			err = fmt.Errorf("transport: WalShipper for replica %s has unknown emit profile %s", replicaID, profile)
		}
		_ = conn.SetWriteDeadline(time.Time{})
		entry.writeMu.Unlock()

		// C1 post-emit hook: on successful wire write, fire the
		// callback installed by the session installer (recovery.Sender)
		// so coord.RecordShipped advances the per-replica shipCursor.
		// Fired OUTSIDE writeMu so the callback doesn't deadlock with
		// itself or block the next writer.
		if err == nil && postEmit != nil {
			postEmit(lsn)
		}
		return err
	}
	s := NewWalShipper(replicaID, HeadSourceFromStorage(e.primaryStore), e.primaryStore, emit)
	// Activate immediately into Realtime — first Ship's lsn > 0 will
	// emit cleanly. Recovery sessions later StartSession to transition
	// to Backlog.
	_ = s.Activate(0)
	entry.shipper = s
	e.walShippers[replicaID] = entry
	return s
}

// updateWalShipperEmitContext sets the wire conn + lineage + profile
// that the per-replica WalShipper's EmitFunc will use on its next
// emit. Ship() calls this before delegating NotifyAppend; recovery
// session start (RecoverySink) calls it to install the dual-lane
// session context.
//
// Ordering invariant (architect P1 review #3): every code path
// that ends in an emit (steady Ship, recovery-session DrainBacklog,
// recovery-session NotifyAppend) MUST refresh this context BEFORE
// the call that may emit. Otherwise EmitFunc emits with stale
// lineage / nil conn / wrong profile.
//
// P2d (architect 2026-04-30): `profile` is part of the context —
// SteadyMsgShip for legacy port, DualLaneWALFrame for dual-lane port.
// The profile selects the encoder; legacy callers pass
// EmitProfileSteadyMsgShip to preserve existing wire shape.
//
// No-op if the WalShipper hasn't been created yet (caller will
// create via WalShipperFor and re-call).
func (e *BlockExecutor) updateWalShipperEmitContext(replicaID string, conn net.Conn, lineage RecoveryLineage, profile EmitProfile) {
	e.walShipperMu.Lock()
	entry, ok := e.walShippers[replicaID]
	e.walShipperMu.Unlock()
	if !ok {
		return
	}
	entry.emitCtxMu.Lock()
	entry.emitConn = conn
	entry.emitLineage = lineage
	entry.emitProfile = profile
	entry.emitCtxMu.Unlock()
}

// SetWalShipperPostEmitHook installs (or clears, if hook==nil) the
// per-emit callback that fires after a successful wire write. Recovery
// sessions use it to translate "this LSN reached the wire" into
// `coord.RecordShipped(replicaID, lsn)` — advances per-peer shipCursor
// in lockstep with emit.
//
// Set BEFORE StartSession so the first emit fires the hook; clear
// (nil) at EndSession so post-session emits don't call into a
// torn-down coordinator state.
//
// No-op if no WalShipperEntry exists for replicaID.
func (e *BlockExecutor) SetWalShipperPostEmitHook(replicaID string, hook func(lsn uint64)) {
	e.walShipperMu.Lock()
	entry, ok := e.walShippers[replicaID]
	e.walShipperMu.Unlock()
	if !ok {
		return
	}
	entry.emitCtxMu.Lock()
	entry.postEmit = hook
	entry.emitCtxMu.Unlock()
}

// WalShipperWriteMu returns the per-replica entry's writeMu — the
// mutex EmitFunc holds during conn.Write. Recovery.Sender uses this
// for its own writeFrame calls during a dual-lane session so both
// writers serialize on the same conn (§6.8 #1 mechanical
// SINGLE-SERIALIZER).
//
// Returns nil when no WalShipperEntry exists for replicaID — caller
// (the sink) returns nil from WriteMu(), and recovery.Sender falls
// back to its own private mutex.
func (e *BlockExecutor) WalShipperWriteMu(replicaID string) *sync.Mutex {
	e.walShipperMu.Lock()
	entry, ok := e.walShippers[replicaID]
	e.walShipperMu.Unlock()
	if !ok {
		return nil
	}
	return &entry.writeMu
}

// SnapshotEmitContext returns the per-replica WalShipper's current
// emit conn + lineage + profile under the same mutex production code
// uses (emitCtxMu). Intended for callers that need to capture the
// steady-state context BEFORE constructing a RecoverySink — so
// EndSession's rule-2 restore lands the right values rather than
// zero / nil.
//
// Returns (nil, zero RecoveryLineage, EmitProfileSteadyMsgShip) when
// no WalShipperEntry exists for replicaID (i.e., no Ship has run yet
// for this replica). That is honest: the steady restore is "no
// context yet, default to steady profile" — matching what a fresh
// Ship() call would set.
//
// Called by: production wiring layer (e.g., transport.startRebuildDualLane)
// right before NewRecoverySink, so the snapshot is consistent with
// what's currently driving Ship().
func (e *BlockExecutor) SnapshotEmitContext(replicaID string) (net.Conn, RecoveryLineage, EmitProfile) {
	e.walShipperMu.Lock()
	entry, ok := e.walShippers[replicaID]
	e.walShipperMu.Unlock()
	if !ok {
		return nil, RecoveryLineage{}, EmitProfileSteadyMsgShip
	}
	entry.emitCtxMu.Lock()
	defer entry.emitCtxMu.Unlock()
	return entry.emitConn, entry.emitLineage, entry.emitProfile
}

// DualLanePrimaryBridge returns the recovery PrimaryBridge plus the
// coordinator replica key when this executor was built with
// NewBlockExecutorWithDualLane. Legacy executors return (_, _, false).
//
// Integration tests (e.g. core/replication/component) use this to drive
// PushLiveWrite during an engine-dispatched rebuild. Production should
// ideally route session-lane writes via a single control plane; this
// accessor exists so tests can close the bridge/coordinator seam without
// duplicating cluster executor construction.
func (e *BlockExecutor) DualLanePrimaryBridge() (*recovery.PrimaryBridge, recovery.ReplicaID, bool) {
	if e.dualLane == nil {
		return nil, "", false
	}
	return e.dualLane.Bridge, e.dualLane.ReplicaID, true
}

// Registry lifecycle note (architect P1 review #4): walShippers
// entries are never torn down today. For replica churn, lineage
// invalidation, or executor reuse across distinct peer
// generations, P3+ may add an explicit Forget(replicaID) /
// teardown hook. Out of scope for MVP since current call sites
// (cmd/blockvolume, tests) construct one BlockExecutor per
// (volume, replica) lifecycle.

func (e *BlockExecutor) registerSession(lineage RecoveryLineage) (*activeSession, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.sessions[lineage.SessionID]; exists {
		return nil, fmt.Errorf("executor: session %d already active", lineage.SessionID)
	}
	session := &activeSession{
		lineage: lineage,
		cancel:  make(chan struct{}),
	}
	e.sessions[lineage.SessionID] = session
	return session, nil
}

func (e *BlockExecutor) attachConn(session *activeSession, conn net.Conn) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	current, ok := e.sessions[session.lineage.SessionID]
	if !ok || current != session {
		return errSessionInvalidated
	}
	session.conn = conn
	return nil
}

func (e *BlockExecutor) detachConn(session *activeSession, conn net.Conn) {
	e.mu.Lock()
	defer e.mu.Unlock()

	current, ok := e.sessions[session.lineage.SessionID]
	if !ok || current != session {
		return
	}
	if session.conn == conn {
		session.conn = nil
	}
}

func (e *BlockExecutor) signalSessionStart(replicaID string, sessionID uint64) {
	e.mu.Lock()
	cb := e.onSessionStart
	_, ok := e.sessions[sessionID]
	e.mu.Unlock()
	if cb == nil || !ok {
		return
	}
	cb(adapter.SessionStartResult{
		ReplicaID: replicaID,
		SessionID: sessionID,
	})
}

func (e *BlockExecutor) finishSession(replicaID string, session *activeSession, achieved uint64, err error) {
	if errors.Is(err, errSessionInvalidated) {
		return
	}

	e.mu.Lock()
	current, ok := e.sessions[session.lineage.SessionID]
	if !ok || current != session {
		e.mu.Unlock()
		return
	}
	delete(e.sessions, session.lineage.SessionID)
	cb := e.onSessionClose
	e.mu.Unlock()

	if cb == nil {
		return
	}
	if err != nil {
		// T4d-1 (architect HIGH v0.1 #1 + v0.3 boundary fix): extract
		// substrate-side RecoveryFailure via errors.As + map to
		// engine-owned RecoveryFailureKind. Engine MUST NOT import
		// core/storage; transport does the mapping at the boundary
		// (transport already imports both packages).
		cb(adapter.SessionCloseResult{
			ReplicaID:   replicaID,
			SessionID:   session.lineage.SessionID,
			Success:     false,
			FailureKind: classifyRecoveryFailure(err),
			FailReason:  err.Error(),
		})
		return
	}
	cb(adapter.SessionCloseResult{
		ReplicaID:   replicaID,
		SessionID:   session.lineage.SessionID,
		Success:     true,
		AchievedLSN: achieved,
	})
}
