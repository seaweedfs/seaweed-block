package transport

import (
	"log"
	"net"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// ApplyHook is the T4d-2 plug-in seam for the replica recovery
// apply gate. When non-nil, the MsgShipEntry handler delegates to
// the hook instead of calling `store.ApplyEntry` directly. The hook
// owns lane discrimination + per-session state + per-LBA stale-skip
// per kickoff §9 (Q1/Q2/Q3) + round-43/44 architect lock.
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
	Apply(lineage RecoveryLineage, lba uint32, data []byte, lsn uint64) error
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
}

// NewReplicaListener creates a listener on the given address.
// No ApplyHook installed — MsgShipEntry handler calls store.ApplyEntry
// directly (preserves T4a/T4b/T4c behavior; existing tests pass
// unchanged).
func NewReplicaListener(addr string, store storage.LogicalStorage) (*ReplicaListener, error) {
	return NewReplicaListenerWithApplyHook(addr, store, nil)
}

// NewReplicaListenerWithApplyHook creates a listener with the T4d-2
// apply-gate plug-in installed. The MsgShipEntry handler delegates
// to `hook.Apply(lineage, lba, data, lsn)` for lane discrimination
// + per-LBA stale-skip + coverage accounting.
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

// Stop shuts down the listener. Safe to call multiple times.
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
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.handleConn(conn)
		}()
	}
}

func (r *ReplicaListener) handleConn(conn net.Conn) {
	defer conn.Close()

	for {
		msgType, payload, err := ReadMsg(conn)
		if err != nil {
			return // connection closed
		}

		switch msgType {
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
			// T4d-2: if the apply-gate is installed, route through
			// it for lane discrimination + per-LBA stale-skip +
			// coverage accounting. The gate reads lineage.TargetLSN
			// to decide live vs recovery (Q2 — no wire byte; existing
			// signal). Live-lane stale entries return error here →
			// log + close conn (round-44
			// INV-REPL-LIVE-LANE-STALE-FAILS-LOUD).
			if r.applyHook != nil {
				if err := r.applyHook.Apply(entry.Lineage, entry.LBA, entry.Data, entry.LSN); err != nil {
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
			// Rebuild blocks carry the engine's frozen targetLSN in their
			// lineage. Apply that real LSN immediately so any future
			// LSN-aware ApplyEntry guard still treats rebuild data as current.
			if err := r.store.ApplyEntry(lba, data, lineage.TargetLSN); err != nil {
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
			// The done message carries the engine's frozen target LSN.
			// Advance the replica's frontier metadata without touching
			// any block data — AdvanceFrontier only updates nextLSN/walHead.
			r.store.AdvanceFrontier(lineage.TargetLSN)
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
		incoming.EndpointVersion == 0 || incoming.TargetLSN == 0 {
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
		return incoming.TargetLSN == active.TargetLSN
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
