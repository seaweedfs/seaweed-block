package transport

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

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

// ApplyHook is the plug-in seam for the replica recovery apply gate.
// The hook is LANE-EXPLICIT: caller (MsgShipEntry handler) decides
// lane from connection/session handler context and invokes the
// matching method. The hook does NOT inspect payload bytes for lane
// discrimination.
//
// Implementations: `replication.ReplicaApplyGate`. Interface lives
// in transport so the gate package can satisfy it without creating
// an import cycle (transport ← replication import direction is the
// existing one; this interface is the duck-type adapter).
//
// Returning a non-nil error → caller (handler) logs + closes conn.
// Per INV-REPL-LIVE-LANE-STALE-FAILS-LOUD: live-lane stale entries
// surface here as errors.
// RecoveryPrefixHook accepts explicit completed-prefix certificates. It does
// not derive source coverage from Sync. Admission is checked at the effect.
type SessionRetirer interface{ CloseSession(sessionID uint64) }

// RecoveryPrefixHook accepts explicit completed-prefix certificates. It does
// not derive source coverage from Sync. Admission is checked at the effect.
type RecoveryPrefixHook interface {
	CertifyRecoveryPrefix(lineage RecoveryLineage) error
	CertifiedPrefix() uint64
}

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
	// retainedBase: the last rebuild was refused because this store still
	// holds an installed BASE (storage.ErrRebuildRetainedBase); cleared by
	// a completed rebuild.
	retainedBase atomic.Bool

	store     storage.LogicalStorage
	listener  net.Listener
	stopCh    chan struct{}
	wg        sync.WaitGroup
	applyHook ApplyHook // optional gate plug-in; nil = direct apply
	// epochs is the volume's epoch table (nil = none): a shipped entry's
	// epoch is noted durably before the entry is applied, and the barrier
	// reply names the last data epoch at the durable end.
	epochs *EpochTable

	mu               sync.Mutex
	activeLineage    RecoveryLineage
	prefixEligible   RecoveryLineage // one active source-covered recovery certificate
	sessionFrames    map[uint64]int
	retiringSessions map[uint64]bool
	// epochFloor is the highest epoch this host has taken a role at
	// (G2): mutating traffic below it is refused. primary is set while
	// this host is the primary: then every inbound replication mutation
	// is refused (it may only come from a deposed primary). Probes are
	// unaffected.
	epochFloor uint64
	primary    bool
	// inFlight counts mutating frames between their gate check and the
	// end of their effect; drained is signalled when it reaches zero.
	inFlight int
	drained  *sync.Cond
	// fenceDrainTimeout bounds how long FenceAsPrimary waits for them.
	fenceDrainTimeout time.Duration
	// G1: the durable floor. requireRole (listeners built with
	// NewReplicaListenerWithFloor) refuses every mutation until a role call
	// has succeeded (assigned). floorPath "" keeps the floor in memory only.
	requireRole bool
	assigned    bool
	volumeID    string
	floorPath   string
	// floorMu serializes floor persistence; persisted is the highest epoch
	// durably recorded, checked under floorMu so a lower write never lands
	// over a higher one. Lock order: floorMu, then mu (briefly); never the
	// reverse, and mu is never held across the file I/O.
	floorMu   sync.Mutex
	persisted uint64
	conns     map[net.Conn]struct{} // active handler connections; protected by mu
}

// NewReplicaListener creates a listener on the given address.
// No ApplyHook installed — MsgShipEntry handler calls store.ApplyEntry
// directly (the simpler no-gate path).
func NewReplicaListener(addr string, store storage.LogicalStorage) (*ReplicaListener, error) {
	return NewReplicaListenerWithApplyHook(addr, store, nil)
}

// NewReplicaListenerWithFloor creates the production listener (fencing gap
// G1). It starts unassigned: every mutating frame is refused until the host
// calls FenceAsPrimary or AdmitAsReplica successfully. With a floorPath the
// epoch floor is loaded from the volume's floor record (InitReplicaFloor
// writes it when the volume is created) and every raise is persisted before
// it takes effect; a missing or invalid record refuses construction. An
// empty floorPath (non-durable memback) keeps the floor in memory.
func NewReplicaListenerWithFloor(addr string, store storage.LogicalStorage, volumeID, floorPath string) (*ReplicaListener, error) {
	return NewReplicaListenerWithFloorAndApplyHook(addr, store, volumeID, floorPath, nil)
}

// NewReplicaListenerWithFloorAndApplyHook preserves role and durable-floor
// admission while using the volume's shared apply ordering gate.
func NewReplicaListenerWithFloorAndApplyHook(addr string, store storage.LogicalStorage, volumeID, floorPath string, hook ApplyHook) (*ReplicaListener, error) {
	var floor uint64
	if floorPath != "" {
		var err error
		if floor, err = loadFloor(floorPath, volumeID); err != nil {
			return nil, err
		}
	}
	r, err := NewReplicaListenerWithApplyHook(addr, store, hook)
	if err != nil {
		return nil, err
	}
	r.requireRole, r.volumeID, r.floorPath = true, volumeID, floorPath
	r.epochFloor, r.persisted = floor, floor
	return r, nil
}

// raiseFloor makes epoch the floor if it is higher: persisted first (when the
// listener has a floor record), then in memory. A persist error leaves both
// unchanged and is returned; the caller must not act on the higher epoch.
func (r *ReplicaListener) raiseFloor(epoch uint64) error {
	r.floorMu.Lock()
	defer r.floorMu.Unlock()
	if epoch > r.persisted {
		if r.floorPath != "" {
			if err := persistFloor(r.floorPath, r.volumeID, epoch); err != nil {
				return err
			}
		}
		r.persisted = epoch
	}
	r.mu.Lock()
	if epoch > r.epochFloor {
		r.epochFloor = epoch
	}
	r.mu.Unlock()
	return nil
}

// admitMutation is the gate for every mutating frame: the lineage check,
// then, if the frame's epoch raises the floor, the durable raise before any
// effect or ACK (G1). It runs inside the frame's counted section (G2), so a
// fence waits for it.
func (r *ReplicaListener) admitMutation(l RecoveryLineage) (bool, string) {
	r.mu.Lock()
	prev := r.activeLineage
	if why := r.mutationLineageRefusalLocked(l); why != "" {
		r.mu.Unlock()
		return false, why
	}
	if r.sessionFrames == nil {
		r.sessionFrames = make(map[uint64]int)
	}
	r.sessionFrames[l.SessionID]++ // register before another lineage can retire it
	r.mu.Unlock()
	if err := r.raiseFloor(l.Epoch); err != nil {
		r.mu.Lock()
		if r.activeLineage == l {
			r.activeLineage = prev
		}
		r.markSessionRetiringLocked(l.SessionID)
		r.mu.Unlock()
		r.endSessionFrame(l.SessionID)
		log.Printf("replica: refuse mutation at epoch %d: %v", l.Epoch, err)
		return false, "epoch floor not durable: " + err.Error()
	}
	r.mu.Lock()
	if prev.SessionID != l.SessionID {
		r.markSessionRetiringLocked(prev.SessionID)
	}
	r.mu.Unlock()
	return true, ""
}

// NewReplicaListenerWithApplyHook creates a listener with the
// apply-gate plug-in installed. The MsgShipEntry handler dispatches
// caller-side to `hook.ApplyRecovery` or `hook.ApplyLive` based on
// the connection/session lane it has established for this entry.
// The hook itself is LANE-PURE — it does not inspect payload bytes
// to decide lane.
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
	r.closeSettledSessions()
}

// StopHard is like Stop but also forcibly closes all active handler
// connections so handlers exit immediately. Used by component tests
// that simulate "replica process died" without ever closing the
// remote (primary) side of the conn — in production, the OS would
// FIN/RST conns from a dead process; on localhost in-process tests,
// nothing closes them, and Stop's wg.Wait would block forever.
//
// Role withdrawal may also use this after FenceAsPrimary has drained all
// admitted mutations: idle peer connections must not hold a withdrawn endpoint.
// Safe to call multiple times. Idempotent.
//
// This seam exists to make the restart-catch-up component test
// deterministic without depending on remote-side teardown.
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
	r.closeSettledSessions()
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
		// Join admission with StopHard's connection snapshot. A connection
		// accepted just before Close must not escape the shutdown snapshot.
		select {
		case <-r.stopCh:
			r.mu.Unlock()
			_ = conn.Close()
			return
		default:
		}
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
	// The legacy single-lane rebuild is the second BASE-install caller
	// (DESIGN r17.4 section 8.7): the first admitted rebuild message of a
	// connection begins one install at its frontier hint under a fresh
	// random ID; a reconnect is a new connection and so a superseding
	// install. Stores without the seam keep their pre-seam behaviour behind
	// the legacy handle (lazy BaseStager staging, WriteExtentDirect,
	// AdvanceFrontier at completion).
	var install storage.InstallHandle
	var installBase uint64
	// recoveryToken is this connection's hold on the epoch table's invalid
	// mark (0 = none): its recovery completes only under it.
	var recoveryToken uint64
	beginInstall := func(hint uint64) error {
		if install != nil {
			if hint != installBase {
				return fmt.Errorf("rebuild frontier hint %d differs from the install's BaseLSN %d", hint, installBase)
			}
			return nil
		}
		// The base's history arrives only with the done frame: the table is
		// invalid from before the first base byte until that replace.
		tok, err := r.epochs.Invalidate()
		if err != nil {
			return err
		}
		recoveryToken = tok
		var id [16]byte
		for id == ([16]byte{}) {
			if _, err := rand.Read(id[:]); err != nil {
				return err
			}
		}
		h, err := storage.BeginInstall(r.store, hint, id)
		if err != nil {
			return err
		}
		install, installBase = h, hint
		return nil
	}

	for {
		msgType, payload, err := ReadMsg(conn)
		if err != nil {
			return // connection closed
		}

		// G2: a mutating frame is counted from before its gate check until its
		// effect is done, so FenceAsPrimary can wait out every frame that may
		// already have passed the gate (review of #82).
		mutating := isMutationMsg(msgType)
		if mutating {
			r.beginMutationFrame()
		}
		// A mutating frame's ack is written after the frame is uncounted:
		// the fence waits for storage effects only, never for the network.
		var replyType byte
		var reply []byte
		var admittedSession uint64
		var refusal string // why the last admit refused (logged by its caller)
		admit := func(l RecoveryLineage) bool {
			ok, why := r.admitMutation(l)
			if !ok {
				refusal = why
				return false
			}
			admittedSession = l.SessionID
			return true
		}
		stop := func() bool {
			switch msgType {
			case MsgRecoveryLaneStart:
				lineage, err := DecodeLineage(payload)
				if err != nil {
					log.Printf("replica: decode recovery lane start: %v", err)
					return true
				}
				if !admit(lineage) {
					log.Printf("replica: reject stale recovery lane start session=%d epoch=%d endpointVersion=%d: %s",
						lineage.SessionID, lineage.Epoch, lineage.EndpointVersion, refusal)
					return true
				}
				// The lane ships history whose epochs arrive only at its
				// final barrier: invalid from before its first entry until
				// that replace.
				// The catch-up states the LSN it starts after (8 bytes after
				// the lineage): under the exclusive fence the replica takes
				// the mark only if its durable end is exactly that LSN, so
				// the start is conditional on what the primary's probe saw.
				if len(payload) < 40 {
					log.Printf("replica: recovery lane start without the LSN it starts after: refused")
					return true
				}
				from := binary.BigEndian.Uint64(payload[32:40])
				check := func() error {
					end, err := r.store.Sync()
					if err != nil {
						return err
					}
					if end != from {
						return fmt.Errorf("catch-up starts after %d, the durable end is %d", from, end)
					}
					return nil
				}
				tok, err := r.epochs.InvalidateIf(check)
				if err != nil {
					log.Printf("replica: recovery lane epoch table: %v", err)
					return true
				}
				recoveryToken = tok
				lane = mutationLaneRecovery
				r.markPrefixEligibility(lineage, false)

			case MsgShipEntry:
				entry, err := DecodeShipEntry(payload)
				if err != nil {
					log.Printf("replica: decode ship entry: %v", err)
					return true
				}
				if !admit(entry.Lineage) {
					log.Printf("replica: reject stale ship session=%d epoch=%d endpointVersion=%d: %s",
						entry.Lineage.SessionID, entry.Lineage.Epoch, entry.Lineage.EndpointVersion, refusal)
					return true
				}
				// Live lane: the row first, then the entry, so an entry
				// whose epoch the table cannot name is never applied. A
				// recovery lane ships history: the session's epoch is not
				// its entries' epoch, so nothing is noted here; the
				// session's final barrier carries the primary's rows.
				// Both lanes apply under the epoch table's fence: a recovery
				// apply while its token owns the mark, a live one while the
				// table is valid (so a recovery's Invalidate drains it and
				// nothing live lands while a recovery owns the store).
				apply := func(fn func() error) error {
					if lane == mutationLaneRecovery {
						return r.epochs.ApplyUnder(recoveryToken, fn)
					}
					return r.epochs.ApplyLiveUnder(fn)
				}
				if lane != mutationLaneRecovery {
					// A new epoch starts exactly at this store's next LSN: a
					// live entry of a new epoch below it would sit over an
					// old tail (blocks of entries the primary never had), a
					// gap above it would skip entries. Refused; the primary
					// reconciles (diverged: rebuild; behind: catch-up).
					if r.epochs.AppendsAt(entry.Lineage.Epoch) && entry.LSN != r.store.NextLSN() {
						log.Printf("replica: live entry of new epoch %d at lsn %d, this store's next lsn is %d: refused", entry.Lineage.Epoch, entry.LSN, r.store.NextLSN())
						return true
					}
					// Inside an epoch an entry is applied only at the position
					// its LSN names, the next one: an entry above it (an LSN
					// skipped) would be held at another position while the
					// replica reports it does not hold it, and the missing LSN
					// could never land below it. Refused and the connection
					// closed; the primary catches up from the replica's
					// barrier end. (A lower LSN is a stale re-ship, handled
					// below.) This holds with the apply gate too: it checks
					// each block's staleness, not the position, and no live
					// entry lands while a recovery owns the store.
					if entry.LSN > r.store.NextLSN() {
						log.Printf("replica: live entry at lsn %d skips ahead of this store's next lsn %d (epoch %d): refused", entry.LSN, r.store.NextLSN(), entry.Lineage.Epoch)
						return true
					}
					if err := r.epochs.Note(entry.Lineage.Epoch, entry.LSN); err != nil {
						log.Printf("replica: epoch table lsn=%d epoch=%d: %v", entry.LSN, entry.Lineage.Epoch, err)
						return true
					}
				}
				// If the apply-gate is installed, route through it.
				// Caller (this handler) decides lane; gate is lane-pure.
				// Live-lane stale entries return error here → log +
				// close conn (INV-REPL-LIVE-LANE-STALE-FAILS-LOUD).
				if r.applyHook != nil {
					err := apply(func() error {
						if lane == mutationLaneRecovery {
							return r.applyHook.ApplyRecovery(entry.Lineage, entry.LBA, entry.Data, entry.LSN)
						}
						return r.applyHook.ApplyLive(entry.Lineage, entry.LBA, entry.Data, entry.LSN)
					})
					if err != nil {
						log.Printf("replica: apply gate: %v", err)
						return true
					}
					return false
				}
				// No hook: direct apply. A failed apply ends the connection, as
				// the apply gate does: no later entry on it is applied, so no
				// barrier can ack a frontier past the missing LSN (#89). The
				// primary sees the closed connection and recovers the replica.
				if err := apply(func() error { return r.store.ApplyEntry(entry.LBA, entry.Data, entry.LSN) }); err != nil {
					log.Printf("replica: apply entry lsn=%d: %v", entry.LSN, err)
					return true
				}

			case MsgProbeReq:
				// ProbeReq carries full RecoveryLineage. Decode + validate
				// + echo via the symmetric-pair rule. Failure to decode
				// (short / zeroed lineage) closes the conn without
				// echoing — the primary times out + treats as
				// unreachable, which is the correct fail-closed surface.
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
					return true
				}
				if !r.validateProbeLineage(req.Lineage) {
					log.Printf("replica: reject stale probe session=%d epoch=%d endpointVersion=%d",
						req.Lineage.SessionID, req.Lineage.Epoch, req.Lineage.EndpointVersion)
					return true
				}
				R, S, H := r.store.Boundaries()
				resp := EncodeProbeResp(ProbeResponse{
					Lineage:   req.Lineage, // echo per symmetric-pair rule
					SyncedLSN: R,
					WalTail:   S,
					WalHead:   H,
				})
				if err := WriteMsg(conn, MsgProbeResp, resp); err != nil {
					return true
				}

			case MsgRebuildBlock:
				lineage, lba, data, err := DecodeRebuildBlock(payload)
				if err != nil {
					log.Printf("replica: decode rebuild block: %v", err)
					return true
				}
				if !admit(lineage) {
					log.Printf("replica: reject stale rebuild block session=%d epoch=%d endpointVersion=%d: %s",
						lineage.SessionID, lineage.Epoch, lineage.EndpointVersion, refusal)
					return true
				}
				// Rebuild blocks are BASE-lane bytes. They must not enter the
				// WAL apply path and must not advance R/S/H per block; the
				// frontier is reconciled once, at MsgRebuildDone.
				if err := beginInstall(lineage.EffectiveFrontierHint()); err != nil {
					log.Printf("replica: begin rebuild base: %v", err)
					r.noteRebuildRefusal(err)
					return true
				}
				if err := r.epochs.ApplyUnder(recoveryToken, func() error { return install.WriteBase(lba, data) }); err != nil {
					log.Printf("replica: apply rebuild base block: %v", err)
					return true
				}

			case MsgRebuildDone:
				lineage, err := DecodeLineage(payload)
				if err != nil {
					log.Printf("replica: decode rebuild done: %v", err)
					return true
				}
				if !admit(lineage) {
					log.Printf("replica: reject stale rebuild done session=%d epoch=%d endpointVersion=%d: %s",
						lineage.SessionID, lineage.Epoch, lineage.EndpointVersion, refusal)
					return true
				}
				// Done (DESIGN 8.7): an empty rebuild begins here; the hint must
				// equal the install's BaseLSN; then complete, then the logical
				// Sync, which is ordinary once installed. A failed completion or
				// Sync closes the connection with no MsgBarrierResp, so the
				// primary never sees an AchievedLSN from a failed call.
				if err := beginInstall(lineage.EffectiveFrontierHint()); err != nil {
					log.Printf("replica: begin rebuild base at done: %v", err)
					r.noteRebuildRefusal(err)
					return true
				}
				if err := r.epochs.ApplyUnder(recoveryToken, install.MarkBaseComplete); err != nil {
					log.Printf("replica: complete rebuild base: %v", err)
					return true
				}
				frontier, err := r.store.Sync()
				if err != nil {
					log.Printf("replica: sync after rebuild done: %v", err)
					return true
				}
				// The rebuilt image is the primary's history up to frontier:
				// its rows cut there replace this table before the ack. A crash
				// before the replace leaves the old rows, which reconcile as
				// diverged: another rebuild, never a wrong count.
				if len(payload) > 32 {
					rows, err := DecodeEpochRows(payload[32:])
					if err != nil {
						log.Printf("replica: rebuild done epoch rows: %v", err)
						return true
					}
					if r.epochs != nil {
						if err := r.epochs.CompleteRecovery(RowsThrough(rows, frontier), recoveryToken, true); err != nil {
							log.Printf("replica: rebuild done epoch table: %v", err)
							return true
						}
					}
				}
				// A completed full BASE supplies the covered starting image.
				r.markPrefixEligibility(lineage, true)
				r.retainedBase.Store(false) // healed: no longer refused
				// Echo the request's full lineage in the rebuild-done ack
				// so primary-side validators can consume it.
				resp := EncodeBarrierResp(BarrierResponse{
					Lineage:          lineage,
					AchievedLSN:      frontier,
					LastDataEpoch:    r.epochs.LastDataRowAt(frontier).Epoch,
					LastDataFirstLSN: r.epochs.LastDataRowAt(frontier).FirstLSN,
					StoreID:          storage.StoreIDOf(r.store),
				})
				replyType, reply = MsgBarrierResp, resp
				return true

			case MsgCertifiedPrefix:
				lineage, err := DecodeLineage(payload)
				if err != nil || r.certifyRecoveryPrefix(lineage) != nil {
					return true
				}

			case MsgBarrierReq:
				lineage, err := DecodeLineage(payload)
				if err != nil {
					log.Printf("replica: decode barrier req: %v", err)
					return true
				}
				if !admit(lineage) {
					log.Printf("replica: reject stale barrier session=%d epoch=%d endpointVersion=%d: %s",
						lineage.SessionID, lineage.Epoch, lineage.EndpointVersion, refusal)
					return true
				}
				frontier, syncErr := r.store.Sync()
				// A catch-up's final barrier carries the primary's rows:
				// the entries up to frontier are now the primary's history,
				// so its rows cut there replace this table before the ack.
				if len(payload) > 32 {
					if syncErr != nil {
						log.Printf("replica: catch-up barrier sync: %v", syncErr)
						return true // no ack: the table is not replaced over an unknown end
					}
					rows, err := DecodeEpochRows(payload[32:])
					if err != nil {
						log.Printf("replica: catch-up barrier epoch rows: %v", err)
						return true
					}
					if r.epochs != nil {
						// A catch-up completes only over a table that was valid
						// when it took the mark (otherwise a rebuild is needed).
						if err := r.epochs.CompleteRecovery(RowsThrough(rows, frontier), recoveryToken, false); err != nil {
							log.Printf("replica: catch-up barrier epoch table: %v", err)
							return true
						}
					}
				}
				// Echo the request's full lineage in the barrier ack.
				// The primary's DurabilityCoordinator validates this
				// tuple against the session it is awaiting before
				// counting the ack toward quorum.
				resp := EncodeBarrierResp(BarrierResponse{
					Lineage:          lineage,
					AchievedLSN:      frontier,
					LastDataEpoch:    r.epochs.LastDataRowAt(frontier).Epoch,
					LastDataFirstLSN: r.epochs.LastDataRowAt(frontier).FirstLSN,
					StoreID:          storage.StoreIDOf(r.store),
				})
				replyType, reply = MsgBarrierResp, resp

			default:
				log.Printf("replica: unknown message type 0x%02x", msgType)
				return true
			}
			return false
		}()
		if admittedSession != 0 {
			r.endSessionFrame(admittedSession)
		}
		if mutating {
			r.endMutationFrame()
		}
		if reply != nil {
			if err := WriteMsg(conn, replyType, reply); err != nil {
				return
			}
		}
		if stop {
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
// Called by: replica's MsgProbeReq handler.
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

// FenceAsPrimary is called when this host takes the primary role at epoch
// (G2), before its frontend serves IO: from now on the listener refuses
// every replication mutation (ship, recovery lane start, rebuild), and none
// below epoch is accepted again. Probes are still answered.
//
// It returns only once every mutating frame that may already have passed
// the gate has finished its effect, so nothing from a deposed primary is
// applied after the host serves as primary. If that does not happen within
// fenceDrainTimeout it returns an error, and the host must not take the
// role (it fails closed; master redelivers the assignment).
//
// G1: after the drain, the floor (at least epoch) is persisted before it
// returns success; a persist error is returned and the role must not be
// adopted. Order: refuse new admissions, drain, persist, then assigned.
func (r *ReplicaListener) FenceAsPrimary(epoch uint64) error {
	if err := r.fenceAndDrain(epoch); err != nil {
		return err
	}
	if err := r.raiseFloor(epoch); err != nil {
		return err
	}
	r.mu.Lock()
	r.assigned = true
	r.mu.Unlock()
	return nil
}

func (r *ReplicaListener) fenceAndDrain(epoch uint64) error {
	return r.floorAndDrain(epoch, true)
}

// FenceFloor is this replica's half of a fencing round (the ack fence): its
// promise that nothing below epoch is applied or acknowledged any more,
// durable before it returns. Order: the floor in memory first (every new
// frame below epoch is refused at once), then the drain of every mutation
// admitted before it (none of them applies or ACKs after the promise), then
// the floor persisted. A drain timeout or a persist error is returned and the
// caller must not answer the round (no promise was made); frames below epoch
// stay refused. The role is unchanged: a line re-issued at epoch to the same
// primary (a failed round's heal) is admitted by AdmitAsReplica as usual.
// A listener in the primary role refuses the fence (ErrFencePrimary) and
// changes nothing: the primary serves its epoch, and a floor above it would
// fence the volume's own writes; whatever the master sends, the node
// decides this itself.
func (r *ReplicaListener) FenceFloor(epoch uint64) error {
	if err := r.floorAndDrain(epoch, false); err != nil {
		return err
	}
	return r.raiseFloor(epoch)
}

// ErrFenceBelowFloor: a primary role below this replica's floor.
var ErrFenceBelowFloor = errors.New("replica listener: a primary role below the epoch floor")

// ErrFencePrimary: a fencing round reached the volume's primary.
var ErrFencePrimary = errors.New("replica listener: a fencing round never fences the primary")

// DurableFloor is the floor persisted in the volume's floor record (in
// memory for a listener without one): the epoch below which this replica
// has promised to apply and acknowledge nothing.
func (r *ReplicaListener) DurableFloor() uint64 {
	r.floorMu.Lock()
	defer r.floorMu.Unlock()
	return r.persisted
}

// floorAndDrain raises the in-memory floor to epoch (and, for a primary
// fence, refuses every mutation), then waits until no mutation admitted
// before it is still in flight.
func (r *ReplicaListener) floorAndDrain(epoch uint64, primary bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if primary && epoch < r.epochFloor {
		// the floor is a promise to take nothing below it: a primary role
		// at a lower epoch is a stale line
		return fmt.Errorf("%w: primary at epoch %d under floor %d", ErrFenceBelowFloor, epoch, r.epochFloor)
	}
	if primary {
		r.primary = true
	} else if r.primary {
		// checked under the lock that raises the floor: a concurrent
		// FenceAsPrimary cannot slip between the check and the raise
		return fmt.Errorf("%w at epoch %d", ErrFencePrimary, epoch)
	}
	if epoch > r.epochFloor {
		r.epochFloor = epoch
	}
	if r.inFlight == 0 {
		return nil
	}
	timeout := r.fenceDrainTimeout
	if timeout <= 0 {
		timeout = defaultFenceDrainTimeout
	}
	drained := r.drainedLocked()
	expired := false
	timer := time.AfterFunc(timeout, func() {
		r.mu.Lock()
		expired = true
		r.mu.Unlock()
		drained.Broadcast()
	})
	defer timer.Stop()
	for r.inFlight > 0 && !expired {
		drained.Wait()
	}
	if r.inFlight > 0 {
		return fmt.Errorf("replica listener: %d replication mutation(s) still in flight %v after fencing at epoch %d", r.inFlight, timeout, epoch)
	}
	return nil
}

// defaultFenceDrainTimeout bounds FenceAsPrimary's wait for in-flight
// mutations (one apply or rebuild extent write, or one barrier fsync).
const defaultFenceDrainTimeout = 10 * time.Second

// isMutationMsg names the frames that pass acceptMutationLineage.
func isMutationMsg(msgType byte) bool {
	switch msgType {
	case MsgRecoveryLaneStart, MsgShipEntry, MsgRebuildBlock, MsgRebuildDone, MsgBarrierReq, MsgCertifiedPrefix:
		return true
	}
	return false
}

// EnterDualLane applies this listener's admission rules to the dual-lane
// rebuild port (recovery.DualLaneGate): no frame on a primary, none below the
// floor, none before the role is known, and a higher epoch is made durable
// before any effect. The returned release ends the counted section a fence
// waits on.
func (r *ReplicaListener) EnterDualLane(epoch uint64) (func(), error) {
	r.beginMutationFrame()
	r.mu.Lock()
	refused := epoch == 0 || r.primary || epoch < r.epochFloor || (r.requireRole && !r.assigned)
	floor := r.epochFloor
	r.mu.Unlock()
	if refused {
		r.endMutationFrame()
		return nil, fmt.Errorf("replica: dual-lane epoch %d refused (floor %d)", epoch, floor)
	}
	if err := r.raiseFloor(epoch); err != nil {
		r.endMutationFrame()
		return nil, fmt.Errorf("replica: dual-lane epoch %d: %w", epoch, err)
	}
	return r.endMutationFrame, nil
}

// BeginEpochRecovery (recovery.EpochRecoveryGate): a dual-lane session
// takes the epoch table's invalid mark under a fresh token at its start,
// before its first data frame.
func (r *ReplicaListener) BeginEpochRecovery() (uint64, error) { return r.epochs.Invalidate() }

// ApplyUnderEpochToken (recovery.EpochRecoveryGate): one apply of a
// dual-lane session's data, under the epoch table's apply fence and only
// while the session's token owns the mark.
func (r *ReplicaListener) ApplyUnderEpochToken(token uint64, fn func() error) error {
	return r.epochs.ApplyUnder(token, fn)
}

// CompleteEpochRecovery (recovery.EpochRecoveryGate): after the session's
// data is synced, the primary's rows cut at end make the table valid, only
// under the session's token.
func (r *ReplicaListener) CompleteEpochRecovery(token uint64, rows []byte, end uint64) error {
	decoded, err := DecodeEpochRows(rows)
	if err != nil {
		return err
	}
	return r.epochs.CompleteRecovery(RowsThrough(decoded, end), token, true) // a dual-lane session installs a full base
}

func (r *ReplicaListener) beginMutationFrame() {
	r.mu.Lock()
	r.inFlight++
	r.mu.Unlock()
}

func (r *ReplicaListener) endMutationFrame() {
	r.mu.Lock()
	r.inFlight--
	if r.inFlight == 0 {
		r.drainedLocked().Broadcast()
	}
	r.mu.Unlock()
}

// drainedLocked returns the in-flight condition variable, creating it on
// first use (some callers build the listener without a constructor).
// Caller holds r.mu.
func (r *ReplicaListener) drainedLocked() *sync.Cond {
	if r.drained == nil {
		r.drained = sync.NewCond(&r.mu)
	}
	return r.drained
}

// AdmitAsReplica is called when this host is admitted as a supporting
// replica at epoch (G2): replication is accepted again, but none below
// epoch (the floor never goes down). G1: the floor is persisted first; on a
// persist error nothing changes (a primary stays fenced, an unassigned
// listener stays unassigned) and the error is returned.
func (r *ReplicaListener) AdmitAsReplica(epoch uint64) error {
	if err := r.raiseFloor(epoch); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.primary = false
	r.assigned = true
	return nil
}

func (r *ReplicaListener) acceptMutationLineage(incoming RecoveryLineage) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.acceptMutationLineageLocked(incoming)
}

func (r *ReplicaListener) acceptMutationLineageLocked(incoming RecoveryLineage) bool {
	return r.mutationLineageRefusalLocked(incoming) == ""
}

// mutationLineageRefusalLocked admits incoming (taking it as the active
// lineage when newer) and returns "", or returns why it is refused, one
// reason per cause, for the caller's log line.
func (r *ReplicaListener) mutationLineageRefusalLocked(incoming RecoveryLineage) string {
	if incoming.SessionID == 0 || incoming.Epoch == 0 || incoming.EndpointVersion == 0 {
		return "incomplete lineage"
	}
	// G2: a primary applies no inbound replication, and no host applies
	// any below the epoch it has taken a role at. G1: a production listener
	// applies nothing before its role is known.
	switch {
	case r.primary:
		return "this listener is a primary"
	case incoming.Epoch < r.epochFloor:
		return fmt.Sprintf("epoch below the floor %d", r.epochFloor)
	case r.requireRole && !r.assigned:
		return "no role assigned yet"
	}

	active := r.activeLineage
	if active.SessionID == 0 {
		r.activeLineage = incoming
		return ""
	}

	switch compareLineage(incoming, active) {
	case 1:
		r.activeLineage = incoming
		return ""
	case 0:
		if incoming.EffectiveFrontierHint() == active.EffectiveFrontierHint() {
			return ""
		}
		return fmt.Sprintf("same lineage with another frontier hint (%d, held %d)", incoming.EffectiveFrontierHint(), active.EffectiveFrontierHint())
	default:
		return fmt.Sprintf("below the held lineage session=%d epoch=%d endpointVersion=%d", active.SessionID, active.Epoch, active.EndpointVersion)
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

// A certificate cannot introduce an unseen session or survive a later one.
// Lock order: listener.mu -> gate.mu -> store boundary read. There is no wire
// or physical I/O here; fencing/session admission cannot cross the effect.
func (r *ReplicaListener) certifyRecoveryPrefix(lineage RecoveryLineage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.primary || (r.requireRole && !r.assigned) || lineage.Epoch < r.epochFloor ||
		lineage.SessionID == 0 || lineage.EffectiveFrontierHint() == 0 || !r.activeLineage.Equivalent(lineage) || !r.prefixEligible.Equivalent(lineage) {
		return fmt.Errorf("replica: uncertified/stale recovery lineage")
	}
	hook, ok := r.applyHook.(RecoveryPrefixHook)
	if !ok {
		return fmt.Errorf("replica: no recovery prefix hook")
	}
	return hook.CertifyRecoveryPrefix(lineage)
}

// A successful scan can extend only an already-covered starting prefix. The
// engine starts catch-up at probed R+1; R is monotone, so observing local R<=P
// at the lane marker is a conservative bound on that starting R. A completed
// full BASE instead supplies its own prefix. A refusal only retains history.
func (r *ReplicaListener) markPrefixEligibility(lineage RecoveryLineage, fullBase bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.activeLineage.Equivalent(lineage) || r.primary || (r.requireRole && !r.assigned) {
		return
	}
	r.prefixEligible = RecoveryLineage{}
	hook, ok := r.applyHook.(RecoveryPrefixHook)
	if !ok {
		return
	}
	synced, _, _ := r.store.Boundaries()
	if fullBase || synced <= hook.CertifiedPrefix() {
		r.prefixEligible = lineage
	}
}

// Retirement tracks logical-session frames, not socket lifetimes. A newer
// session may run while an admitted older frame is parked before the hook.
// Lock order remains listener.mu -> gate.mu; versions and P are not released.
func (r *ReplicaListener) markSessionRetiringLocked(id uint64) {
	if id == 0 {
		return
	}
	if r.retiringSessions == nil {
		r.retiringSessions = make(map[uint64]bool)
	}
	r.retiringSessions[id] = true
	r.retireSessionIfSettledLocked(id)
}
func (r *ReplicaListener) retireSessionIfSettledLocked(id uint64) {
	if !r.retiringSessions[id] || id == r.activeLineage.SessionID || r.sessionFrames[id] != 0 {
		return
	}
	if hook, ok := r.applyHook.(SessionRetirer); ok {
		hook.CloseSession(id)
	}
	delete(r.retiringSessions, id)
}
func (r *ReplicaListener) endSessionFrame(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessionFrames[id]--
	if r.sessionFrames[id] == 0 {
		delete(r.sessionFrames, id)
	}
	r.retireSessionIfSettledLocked(id)
}
func (r *ReplicaListener) closeSettledSessions() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if hook, ok := r.applyHook.(SessionRetirer); ok {
		hook.CloseSession(r.activeLineage.SessionID)
		for id := range r.retiringSessions {
			hook.CloseSession(id)
		}
	}
	r.retiringSessions = nil
}

// RebuildRetainedBase is the typed reason this replica cannot heal: its
// last rebuild was refused because the store still holds an installed BASE
// (a replica removed and added back with its older store, a v1 limit).
// "" otherwise.
func (r *ReplicaListener) RebuildRetainedBase() string {
	if r != nil && r.retainedBase.Load() {
		return RebuildRetainedBaseReason
	}
	return ""
}

// RebuildRetainedBaseReason is the typed reason RebuildRetainedBase reports.
const RebuildRetainedBaseReason = "rebuild_retained_base"

func (r *ReplicaListener) noteRebuildRefusal(err error) {
	if errors.Is(err, storage.ErrRebuildRetainedBase) {
		if !r.retainedBase.Swap(true) {
			log.Printf("replica: %s: %v", RebuildRetainedBaseReason, storage.ErrRebuildRetainedBase)
		}
	}
}
