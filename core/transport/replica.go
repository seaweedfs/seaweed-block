package transport

import (
	"log"
	"net"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// liveShipTargetLSNSentinel is the historical steady-state live-ship
// lineage placeholder (== `replication.liveShipTargetLSN`). It is kept
// for tests and wire compatibility only; replica lane discrimination is
// connection-context based, not TargetLSN based.
const liveShipTargetLSNSentinel uint64 = 1

type mutationLane int

const (
	mutationLaneLive mutationLane = iota
	mutationLaneRecovery
)

// ApplyHook is the T4d-2 plug-in seam for the replica recovery
// apply gate. The hook is LANE-EXPLICIT per round-46 architect
// ruling: caller (MsgShipEntry handler) decides lane from
// connection/session handler context and invokes the matching
// method. The hook does NOT inspect payload bytes for lane
// discrimination.
//
// Implementations: `replication.ReplicaApplyGate`. Interface lives
// in transport so the gate package can satisfy it without creating
// an import cycle (transport ← replication import direction is the
// existing one; this interface is the duck-type adapter).
//
// Returning a non-nil error → caller (handler) logs + closes conn.
// Per round-44 INV-REPL-LIVE-LANE-STALE-FAILS-LOUD: live-lane stale
// entries surface here as errors.
type ApplyHook interface {
	// ApplyRecovery routes the entry through the recovery-lane apply
	// path. Stale entries (LSN <= per-LBA applied) skip data + advance
	// recovery coverage.
	ApplyRecovery(lineage RecoveryLineage, lba uint32, data []byte, lsn uint64) error
	// ApplyLive routes the entry through the live-lane apply path.
	// Stale entries return error (fail-loud).
	ApplyLive(lineage RecoveryLineage, lba uint32, data []byte, lsn uint64) error
}

// ReplicaListener accepts connections from the primary and handles
// WAL shipping, probe requests, and rebuild streams against a
// replica-side LogicalStorage.
type ReplicaListener struct {
	store     storage.LogicalStorage
	listener  net.Listener
	stopCh    chan struct{}
	wg        sync.WaitGroup
	applyHook ApplyHook // T4d-2: optional gate plug-in; nil = direct apply

	mu            sync.Mutex
	activeLineage RecoveryLineage
	conns         map[net.Conn]struct{} // active handler connections; protected by mu
}

// NewReplicaListener creates a listener on the given address.
// No ApplyHook installed — MsgShipEntry handler calls store.ApplyEntry
// directly (preserves T4a/T4b/T4c behavior; existing tests pass
// unchanged).
func NewReplicaListener(addr string, store storage.LogicalStorage) (*ReplicaListener, error) {
	return NewReplicaListenerWithApplyHook(addr, store, nil)
}

// NewReplicaListenerWithApplyHook creates a listener with the T4d-2
// apply-gate plug-in installed. The MsgShipEntry handler dispatches
// caller-side to `hook.ApplyRecovery` or `hook.ApplyLive` based on
// the connection/session lane it has established for this entry. The
// hook itself is LANE-PURE per round-46 architect ruling — it does
// not inspect payload bytes to decide lane.
//
// Lane decision comes from handler context:
//   - default legacy SWRP MsgShipEntry connections are live lane
//   - StartCatchUp sends MsgRecoveryLaneStart on its connection before
//     WAL frames; that marker flips this handler to recovery lane
//
// The hook remains lane-pure: it never inspects TargetLSN to decide
// ApplyRecovery vs ApplyLive.
//
// Pass nil hook to get the no-gate behavior (== NewReplicaListener).
func NewReplicaListenerWithApplyHook(addr string, store storage.LogicalStorage, hook ApplyHook) (*ReplicaListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &ReplicaListener{
		store:     store,
		listener:  ln,
		stopCh:    make(chan struct{}),
		applyHook: hook,
		conns:     make(map[net.Conn]struct{}),
	}, nil
}

// Addr returns the listener's address.
func (r *ReplicaListener) Addr() string {
	return r.listener.Addr().String()
}

// Serve starts accepting connections.
func (r *ReplicaListener) Serve() {
	r.wg.Add(1)
	go r.acceptLoop()
}

// Stop shuts down the listener. Safe to call multiple times. Closes
// the listener (no new accepts) and waits for in-flight handlers to
// drain. Handlers will exit on their own when the remote side closes
// the conn or sends a frame; if you need to force handler exit
// regardless of remote state (e.g., simulating replica-process death
// in tests), use StopHard.
//
// Production / operational guidance: prefer Stop for normal shutdown
// (process exit, role change, volume close) — handlers exit cleanly
// after the OS-level FIN/RST from the remote primary's process exit
// or transport timeout. StopHard is intended for test fault-injection
// and exceptional emergency-stop paths only; using it as the default
// shutdown route on production deployments would mask graceful-drain
// bugs that real-world primaries depend on.
func (r *ReplicaListener) Stop() {
	select {
	case <-r.stopCh:
		return // already stopped
	default:
	}
	close(r.stopCh)
	r.listener.Close()
	r.wg.Wait()
}

// StopHard is like Stop but also forcibly closes all active handler
// connections so handlers exit immediately. Used by component tests
// that simulate "replica process died" without ever closing the
// remote (primary) side of the conn — in production, the OS would
// FIN/RST conns from a dead process; on localhost in-process tests,
// nothing closes them, and Stop's wg.Wait would block forever.
//
// Safe to call multiple times. Idempotent.
//
// G5-5C #6 introduced this seam to make the restart-catch-up
// component test deterministic without depending on remote-side
// teardown.
func (r *ReplicaListener) StopHard() {
	select {
	case <-r.stopCh:
		return
	default:
	}
	close(r.stopCh)
	r.listener.Close()
	// Snapshot + close active conns. Handler goroutines will return
	// from ReadMsg with an error and run their deferred conn.Close().
	r.mu.Lock()
	conns := make([]net.Conn, 0, len(r.conns))
	for c := range r.conns {
		conns = append(conns, c)
	}
	r.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	r.wg.Wait()
}

func (r *ReplicaListener) acceptLoop() {
	defer r.wg.Done()
	for {
		conn, err := r.listener.Accept()
		if err != nil {
			select {
			case <-r.stopCh:
				return
			default:
				log.Printf("replica: accept error: %v", err)
				return
			}
		}
		r.mu.Lock()
		r.conns[conn] = struct{}{}
		r.mu.Unlock()
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			defer func() {
				r.mu.Lock()
				delete(r.conns, conn)
				r.mu.Unlock()
			}()
			r.handleConn(conn)
		}()
	}
}

func (r *ReplicaListener) handleConn(conn net.Conn) {
	defer conn.Close()
	lane := mutationLaneLive

	for {
		msgType, payload, err := ReadMsg(conn)
		if err != nil {
			return // connection closed
		}

		switch msgType {
		case MsgRecoveryLaneStart:
			lineage, err := DecodeLineage(payload)
			if err != nil {
				log.Printf("replica: decode recovery lane start: %v", err)
				return
			}
			if !r.acceptMutationLineage(lineage) {
				log.Printf("replica: reject stale recovery lane start session=%d epoch=%d endpointVersion=%d",
					lineage.SessionID, lineage.Epoch, lineage.EndpointVersion)
				return
			}
			lane = mutationLaneRecovery

		case MsgShipEntry:
			entry, err := DecodeShipEntry(payload)
			if err != nil {
				log.Printf("replica: decode ship entry: %v", err)
				return
			}
			if !r.acceptMutationLineage(entry.Lineage) {
				log.Printf("replica: reject stale ship session=%d epoch=%d endpointVersion=%d",
					entry.Lineage.SessionID, entry.Lineage.Epoch, entry.Lineage.EndpointVersion)
				return
			}
			// T4d-2 + round-46 rework: if the apply-gate is installed,
			// route through it. Caller (this handler) decides lane;
			// gate is lane-pure. Live-lane stale entries return error
			// here → log + close conn (round-44
			// INV-REPL-LIVE-LANE-STALE-FAILS-LOUD).
			if r.applyHook != nil {
				var err error
				if lane == mutationLaneRecovery {
					err = r.applyHook.ApplyRecovery(entry.Lineage, entry.LBA, entry.Data, entry.LSN)
				} else {
					err = r.applyHook.ApplyLive(entry.Lineage, entry.LBA, entry.Data, entry.LSN)
				}
				if err != nil {
					log.Printf("replica: apply gate: %v", err)
					return
				}
				continue
			}
			// No hook: T4a/T4b/T4c behavior — direct apply.
			if err := r.store.ApplyEntry(entry.LBA, entry.Data, entry.LSN); err != nil {
				log.Printf("replica: apply entry: %v", err)
			}

		case MsgProbeReq:
			// T4c-1 wire upgrade: ProbeReq now carries full
			// RecoveryLineage. Decode + validate + echo per round-26
			// symmetric-pair rule. Failure to decode (short / zeroed
			// lineage) closes the conn without echoing — the primary
			// times out + treats as unreachable, which is the correct
			// fail-closed surface.
			//
			// Probe is non-mutating: validation gates zeros and clearly
			// stale lineages but MUST NOT advance `activeLineage`.
			// Advancing activeLineage from a probe would cause a
			// later catch-up / rebuild session at a lower sessionID
			// (but same epoch / endpointVersion) to be incorrectly
			// rejected as stale — probes monotonically advance
			// sessionID via the adapter's global counter, so they
			// would routinely race ahead of in-flight session IDs.
			// Use `validateProbeLineage` instead.
			req, err := DecodeProbeReq(payload)
			if err != nil {
				log.Printf("replica: decode probe req: %v", err)
				return
			}
			if !r.validateProbeLineage(req.Lineage) {
				log.Printf("replica: reject stale probe session=%d epoch=%d endpointVersion=%d",
					req.Lineage.SessionID, req.Lineage.Epoch, req.Lineage.EndpointVersion)
				return
			}
			R, S, H := r.store.Boundaries()
			resp := EncodeProbeResp(ProbeResponse{
				Lineage:   req.Lineage, // echo per round-26 Item C.3
				SyncedLSN: R,
				WalTail:   S,
				WalHead:   H,
			})
			if err := WriteMsg(conn, MsgProbeResp, resp); err != nil {
				return
			}

		case MsgRebuildBlock:
			lineage, lba, data, err := DecodeRebuildBlock(payload)
			if err != nil {
				log.Printf("replica: decode rebuild block: %v", err)
				return
			}
			if !r.acceptMutationLineage(lineage) {
				log.Printf("replica: reject stale rebuild block session=%d epoch=%d endpointVersion=%d",
					lineage.SessionID, lineage.Epoch, lineage.EndpointVersion)
				return
			}
			// Rebuild blocks carry the engine's frozen base frontier hint in
			// their lineage. Apply that real LSN immediately so any future
			// LSN-aware ApplyEntry guard still treats rebuild data as current.
			if err := r.store.ApplyEntry(lba, data, lineage.EffectiveFrontierHint()); err != nil {
				log.Printf("replica: apply rebuild block: %v", err)
			}

		case MsgRebuildDone:
			lineage, err := DecodeLineage(payload)
			if err != nil {
				log.Printf("replica: decode rebuild done: %v", err)
				return
			}
			if !r.acceptMutationLineage(lineage) {
				log.Printf("replica: reject stale rebuild done session=%d epoch=%d endpointVersion=%d",
					lineage.SessionID, lineage.Epoch, lineage.EndpointVersion)
				return
			}
			// The done message carries the engine's frozen base frontier hint.
			// Advance the replica's frontier metadata without touching
			// any block data — AdvanceFrontier only updates nextLSN/walHead.
			r.store.AdvanceFrontier(lineage.EffectiveFrontierHint())
			frontier, _ := r.store.Sync()
			// Echo the request's full lineage in the rebuild-done ack
			// per T4b-1 wire extension. T4b scope does not yet validate
			// this lineage on the primary (catch-up/rebuild paths are
			// T4c / T5), but the wire must already carry it so those
			// validators can consume it when they land.
			resp := EncodeBarrierResp(BarrierResponse{
				Lineage:     lineage,
				AchievedLSN: frontier,
			})
			if err := WriteMsg(conn, MsgBarrierResp, resp); err != nil {
				return
			}
			return

		case MsgBarrierReq:
			lineage, err := DecodeLineage(payload)
			if err != nil {
				log.Printf("replica: decode barrier req: %v", err)
				return
			}
			if !r.acceptMutationLineage(lineage) {
				log.Printf("replica: reject stale barrier session=%d epoch=%d endpointVersion=%d",
					lineage.SessionID, lineage.Epoch, lineage.EndpointVersion)
				return
			}
			frontier, _ := r.store.Sync()
			// Echo the request's full lineage in the barrier ack per
			// T4b-1 wire extension (round-21 uniform rule + H5 LOCK).
			// The primary's DurabilityCoordinator (T4b-2/T4b-3)
			// validates this tuple against the session it is awaiting
			// before counting the ack toward quorum.
			resp := EncodeBarrierResp(BarrierResponse{
				Lineage:     lineage,
				AchievedLSN: frontier,
			})
			WriteMsg(conn, MsgBarrierResp, resp)

		default:
			log.Printf("replica: unknown message type 0x%02x", msgType)
			return
		}
	}
}

// validateProbeLineage gates probe lineages: rejects zeros and clearly
// stale tuples (older epoch / endpointVersion than activeLineage), but
// does NOT advance activeLineage. Probe is non-mutating; activeLineage
// belongs to mutating-flow tracking only.
//
// Stale rule: probe is rejected only when its (epoch, endpointVersion)
// pair is strictly older than activeLineage's. SessionID alone does
// NOT determine staleness for probes — probe sessionIDs come from a
// monotonic adapter counter and routinely outpace in-flight session
// IDs at the same (epoch, endpointVersion). A probe at the same
// (epoch, endpointVersion) as activeLineage is always accepted for
// echo, regardless of sessionID ordering.
//
// Called by: replica's MsgProbeReq handler (T4c-1).
// Owns: the read of activeLineage; no writes.
// Borrows: nothing.
func (r *ReplicaListener) validateProbeLineage(incoming RecoveryLineage) bool {
	if incoming.SessionID == 0 || incoming.Epoch == 0 ||
		incoming.EndpointVersion == 0 || incoming.EffectiveFrontierHint() == 0 {
		return false
	}
	r.mu.Lock()
	active := r.activeLineage
	r.mu.Unlock()
	if active.SessionID == 0 {
		// No active mutating lineage yet — any well-formed probe is
		// acceptable for echo.
		return true
	}
	// Reject only on strictly-older (epoch, endpointVersion). Same
	// epoch / endpointVersion is accepted regardless of sessionID.
	if incoming.Epoch < active.Epoch {
		return false
	}
	if incoming.Epoch == active.Epoch && incoming.EndpointVersion < active.EndpointVersion {
		return false
	}
	return true
}

func (r *ReplicaListener) acceptMutationLineage(incoming RecoveryLineage) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if incoming.SessionID == 0 || incoming.Epoch == 0 || incoming.EndpointVersion == 0 {
		return false
	}

	active := r.activeLineage
	if active.SessionID == 0 {
		r.activeLineage = incoming
		return true
	}

	switch compareLineage(incoming, active) {
	case 1:
		r.activeLineage = incoming
		return true
	case 0:
		return incoming.EffectiveFrontierHint() == active.EffectiveFrontierHint()
	default:
		return false
	}
}

func compareLineage(a, b RecoveryLineage) int {
	switch {
	case a.Epoch < b.Epoch:
		return -1
	case a.Epoch > b.Epoch:
		return 1
	}
	switch {
	case a.EndpointVersion < b.EndpointVersion:
		return -1
	case a.EndpointVersion > b.EndpointVersion:
		return 1
	}
	switch {
	case a.SessionID < b.SessionID:
		return -1
	case a.SessionID > b.SessionID:
		return 1
	default:
		return 0
	}
}
