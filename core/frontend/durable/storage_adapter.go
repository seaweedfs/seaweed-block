// Package durable implements the T3a bridge between V3
// `core/storage.LogicalStorage` and `core/frontend.Backend`.
//
// `StorageBackend` wraps any `LogicalStorage` (walstore, smartwal,
// or any future impl) and exposes the `frontend.Backend` surface
// iSCSI and NVMe consume. Responsibilities:
//
//  1. byte ↔ LBA translation (G-int.1).
//     Frontend layer speaks byte offsets + length; storage layer
//     speaks LBA + full-block data. The adapter bridges: partial-
//     block writes trigger read-modify-write; full-block writes
//     skip the read. Reads copy the slice of the backing block
//     covered by the byte range.
//
//  2. Per-I/O fence check (G-int.2).
//     Re-reads `ProjectionView` on every operation; ANY drift
//     (identity field mismatch or Healthy=false) → ErrStalePrimary.
//     Preserves INV-FRONTEND-002.{EPOCH,EV,REPLICA,HEALTHY} under
//     a durable backend.
//
//  3. Operational gate (G-int.3, Addendum A).
//     Before SetOperational(true, _) is called, every I/O returns
//     ErrNotReady with evidence. Starts non-operational — host/
//     provider flips to true after recovery succeeds. This is
//     local readiness only; NEVER advances epoch / mints authority.
//
//  4. Sync (G-int.2 sync policy locked as explicit method).
//     Dispatches to LogicalStorage.Sync and returns error
//     verbatim. iSCSI SYNCHRONIZE CACHE + NVMe Flush wiring lands
//     in T3b.
//
// StorageBackend does NOT own the LogicalStorage lifecycle — the
// Provider (T3b) opens and closes storage. Backend.Close marks the
// handle closed but does not tear down storage.
//
// Boundary guarantee: this package does not import core/authority
// or core/adapter (see core/frontend/boundary_guard_test.go).
package durable

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/iolatency"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// WriteAckPolicy controls whether per-write replication observer failures
// affect the foreground Write result.
type WriteAckPolicy int

const (
	// WriteAckBestEffort is the MVP default: local durable write success is
	// enough for Write to return success. Replication observer failures are
	// logged and left to recovery/progress policy.
	WriteAckBestEffort WriteAckPolicy = iota

	// WriteAckRequireObserverAck is the future full-ack/quorum seam: a Write
	// must have an installed observer and the observer must accept the write.
	// A replica in recovery/down state should surface through observer error,
	// not be silently treated as full-sync success.
	WriteAckRequireObserverAck
)

// ErrReplicationAckUnavailable is returned when WriteAckRequireObserverAck
// cannot obtain the configured replication acknowledgement.
var ErrReplicationAckUnavailable = errors.New("durable: replication ack unavailable")

// WriteObserver is the narrow seam StorageBackend uses to notify the
// replication layer of each successfully-applied local write and to
// delegate Sync durability closure. The ReplicationVolume
// (core/replication) satisfies this interface; the indirection keeps
// core/frontend/durable from importing replication directly and lets
// tests inject mocks.
//
// Observe contract (matches V2 BlockVol.shipMu → ShipAll invariant):
//   - StorageBackend calls Observe AFTER LogicalStorage.Write
//     returns successfully, with the LBA + block data + LSN the
//     storage layer just assigned.
//   - In the default WriteAckBestEffort policy, Observe error does
//     NOT fail the Write (best-effort replication matches V2
//     fire-and-forget ShipAll; per-peer degradation is the observer's
//     concern, not the backend's).
//   - In WriteAckRequireObserverAck, missing observer or Observe
//     error fails Write with ErrReplicationAckUnavailable. This is the
//     RF=2/full-ack seam: a recovering/down replica must not be
//     silently treated as a full-sync peer.
//   - StorageBackend calls Observe from within writeBytes; the
//     ReplicationVolume resequences callbacks by storage-assigned LSN,
//     then dispatches through one ordered queue per peer lineage.
//
// Sync contract (T4b-5 — matches V2 BlockVol.MakeDistributedSync):
//   - When an observer is installed, StorageBackend.Sync DELEGATES
//     the full sync job to observer.Sync — the observer owns both
//     the local LogicalStorage.Sync call and the peer barriers, with
//     barriers ordered behind prior writes in each peer queue.
//   - targetLSN is the primary's current walHead at the moment the
//     host requested Sync; caller (StorageBackend) reads it from
//     LogicalStorage.Boundaries().
//   - Observer.Sync returns error iff the mode-dependent durability
//     arithmetic declared failure (ErrDurabilityBarrierFailed /
//     ErrDurabilityQuorumLost / local fsync error). StorageBackend
//     surfaces the error verbatim so the host sees the real
//     durability outcome.
type WriteObserver interface {
	Observe(ctx context.Context, lba uint32, lsn uint64, data []byte) error
	Sync(ctx context.Context, targetLSN uint64) error
}

// ShipLSNRebaser is implemented by a write observer that orders writes by
// LSN from a next LSN it cached from the store. Whoever moves the store's
// next LSN (a restored host's frontier seed) rebases it before I/O.
type ShipLSNRebaser interface {
	RebaseShipLSN(lsn uint64) error
}

// BatchWriteObserver lets a replication observer accept a contiguous batch of
// locally-committed blocks before waiting for acknowledgement. StorageBackend
// only enables strict-ACK WriteBatch when the installed observer implements
// this interface.
type BatchWriteObserver interface {
	WriteObserver
	ObserveBatch(ctx context.Context, startLBA uint32, lsns []uint64, blocks [][]byte) error
}

// errInvalidOffset is returned for negative byte offsets. Callers
// should never produce these; both iSCSI and NVMe clamp LBA before
// it reaches the backend. Kept as a distinct sentinel so the cause
// is visible in logs when a protocol bug does emit one.
var errInvalidOffset = errors.New("durable: invalid (negative) offset")

// errSyncBeforeOpen is returned if Sync is called before
// SetOperational(true, _). Distinct from ErrNotReady so diagnostic
// logs can tell "host sent FLUSH before we were ready" apart from
// "I/O before first-open".
var errSyncBeforeOpen = errors.New("durable: sync before operational")

const maxFullBlockBatchBlocks = 64

// StorageBackend adapts a LogicalStorage into frontend.Backend.
type StorageBackend struct {
	// testBeforeIOLock, test-only, runs in admit after the readiness check,
	// right before the I/O lock (an arrival handshake for queued I/O).
	testBeforeIOLock func()
	// testAfterGate, test-only, runs in admit after the gate passed and
	// before the I/O registers under mu.
	testAfterGate func()
	// stopFlipAt is when admission last closed for a stop, taken under mu
	// just after operational went false; guarded by mu. An I/O whose gate
	// saw operational before it was admitted (and StopIO drains it); every
	// gate after it refuses.
	stopFlipAt              time.Time
	testPrecopyQueued       func()               // after FIFO registration, outside mu
	testPrecopyGranted      func(*precopyWaiter) // FIFO grant under mu; nonblocking test hook
	testPrecopyStopFallback func()               // before fallback lock attempts, outside mu
	// Runs outside locks after a read registers its readiness notification.
	testBeforeReadReadyWait func()
	testAfterAttachRefusal  func()

	storage   storage.LogicalStorage
	view      frontend.ProjectionView
	id        frontend.Identity
	blockSize int

	// operational is the T3a Addendum A readiness gate. Starts
	// false; provider flips true after Recover() succeeds (T3b).
	// opEvidence is the reason reported when operational=false
	// (e.g., "awaiting recovery", "local epoch ahead of assignment").
	// Readiness waits cover pending chunk attachment and temporary pre-copy freezes.
	// Readiness flags and readyChanged are protected by mu.
	waitAttachReady bool
	precopy         *precopyAdmission // under mu; owns transient admission and FIFO waiters
	admittedIO      int               // under mu; operations past the complete gate
	readyChanged    chan struct{}
	operational     atomic.Bool
	opEvidence      atomic.Value // string

	// ioMu is the I/O admission boundary: every Read, Write and Sync holds
	// its read side for the whole operation, gate included, so StopIO can
	// wait for the I/O admitted before a stop by taking the write side.
	// admit checks the gate once before the read lock too, so I/O arriving
	// while a drain waits fails at once instead of queueing behind it.
	ioMu sync.RWMutex
	// drain, under drainMu, is closed once the pending StopIO drain has
	// taken and released ioMu; nil when none is pending. Every StopIO
	// waits on the same drain, so timed-out retries never pile up waiters.
	drainMu sync.Mutex
	drain   chan struct{}

	mu          sync.Mutex
	closed      bool
	chunkFenced func() bool // installed under mu; called only after releasing mu
	// observerErr is a refused write-observer install (its ship order
	// could not follow the store's next LSN). While set, every I/O is
	// refused, so a volume never serves without its observer. Under mu.
	observerErr error
	// observerMu serializes moving the store's next LSN (the frontier
	// seed) together with rebasing the observer, against installing an
	// observer together with its rebase: whichever runs second sees the
	// other's result. Lock order: observerMu, then mu.
	observerMu    sync.Mutex
	observer      WriteObserver // optional; nil = no replication fan-out
	writeAck      WriteAckPolicy
	profile       writeProfile
	lbaWriteLocks [256]sync.Mutex
}

// NewStorageBackend constructs a backend. Starts NON-operational
// per INV-DURABLE-OPGATE-001; caller must SetOperational(true, _)
// before I/O will succeed.
//
// The storage's block size is captured once at construction
// (LogicalStorage.BlockSize() is a stable geometry field; no T3
// impl changes it mid-session).
func NewStorageBackend(s storage.LogicalStorage, view frontend.ProjectionView, id frontend.Identity) *StorageBackend {
	b := &StorageBackend{
		storage:   s,
		view:      view,
		id:        id,
		blockSize: s.BlockSize(),
	}
	b.opEvidence.Store("awaiting first SetOperational")
	// operational.Store(false) is zero-value default; be explicit.
	b.operational.Store(false)
	return b
}

// Identity returns the captured lineage. Stable once the backend is
// promoted out of zero-state via SetIdentity (or initialized via
// NewStorageBackend with a non-zero id).
func (b *StorageBackend) Identity() frontend.Identity {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.id
}

// SetIdentity latches the backend's lineage from the engine-side
// authoritative projection. This is the bridge for the G5-5 wiring
// where DurableProvider.EnsureStorage constructs the backend BEFORE
// the assignment fact lands.
//
// Pre-assignment state shape (round-11 finding): wrap() captures
// `view.Projection()` at construction. AdapterProjectionView reports
// VolumeID/ReplicaID from CLI config (non-empty even pre-assignment)
// but Epoch=0/EV=0 (engine state is zero-value until OnAssignment
// runs). So a freshly-constructed backend has b.id = {v1, r1, 0, 0}
// — partially-zero, not all-zero. The latch condition keys on
// Epoch=0 (the meaningful "no authority line yet" indicator);
// VolumeID/ReplicaID match is asserted but unchanged.
//
// Semantic: latch when Epoch is currently zero (transitions from
// pre-assignment to first authoritative lineage). After that, only
// master-minted advances for the SAME volume/replica are accepted
// (epoch increase, or endpoint-version increase within the same
// epoch). Cross-volume, cross-replica, or backward lineage changes
// are rejected so old primaries still fail closed via lineageCheck →
// ErrStalePrimary.
//
// Returns true if the identity changed. Returns false on no-op and
// on rejected drift.
func (b *StorageBackend) SetIdentity(id frontend.Identity) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	// VolumeID/ReplicaID came from CLI config at backend construction
	// time and don't change; refuse silently if caller tries to install
	// a different volume/replica (paranoia — should never happen in
	// production).
	if (b.id.VolumeID != "" && b.id.VolumeID != id.VolumeID) ||
		(b.id.ReplicaID != "" && b.id.ReplicaID != id.ReplicaID) {
		return false
	}
	if b.id == id {
		return false
	}
	if b.id.Epoch != 0 || b.id.EndpointVersion != 0 {
		if id.VolumeID != b.id.VolumeID || id.ReplicaID != b.id.ReplicaID {
			return false
		}
		if id.Epoch < b.id.Epoch || (id.Epoch == b.id.Epoch && id.EndpointVersion <= b.id.EndpointVersion) {
			return false
		}
	}
	b.id = id
	log.Printf("durable: StorageBackend.SetIdentity latched volume=%s replica=%s epoch=%d ev=%d",
		id.VolumeID, id.ReplicaID, id.Epoch, id.EndpointVersion)
	return true
}

func (b *StorageBackend) durableStatus(volumeID, path, impl string) VolumeStatus {
	b.mu.Lock()
	id := b.id
	closed := b.closed
	b.mu.Unlock()
	durableLSN, retainedLSN, headLSN := b.storage.Boundaries()
	evidence, _ := b.opEvidence.Load().(string)
	var warnings []string
	var holes uint64
	if w, ok := b.storage.(storage.RecoveryWarner); ok {
		warnings = w.RecoveryWarnings()
	}
	if c, ok := b.storage.(interface{ HolesSuspected() uint64 }); ok {
		holes = c.HolesSuspected()
	}
	var dataSync *storage.DataSyncStatus
	if reporter, ok := b.storage.(interface{ DataSyncPolicy() storage.DataSyncStatus }); ok {
		snapshot := reporter.DataSyncPolicy()
		dataSync = &snapshot
	}
	var checkpoint *storage.CheckpointPersistenceStatus
	if reporter, ok := b.storage.(interface {
		CheckpointPersistence() storage.CheckpointPersistenceStatus
	}); ok {
		snapshot := reporter.CheckpointPersistence()
		checkpoint = &snapshot
	}
	writeProfile := b.WriteProfile()
	if instrumented, ok := b.storage.(storage.WriteInstrumented); ok {
		storageProfile := instrumented.WriteInstrumentation()
		writeProfile.WALCopyOps = storageProfile.WALCopyOps
		writeProfile.WALCopyBytes = storageProfile.WALCopyBytes
		writeProfile.WALCopyDurationNanos = storageProfile.WALCopyDurationNanos
		writeProfile.WALEncodeOps = storageProfile.WALEncodeOps
		writeProfile.WALEncodeBytes = storageProfile.WALEncodeBytes
		writeProfile.WALEncodeDurationNanos = storageProfile.WALEncodeDurationNanos
		writeProfile.WALChecksumOps = storageProfile.WALChecksumOps
		writeProfile.WALChecksumBytes = storageProfile.WALChecksumBytes
		writeProfile.WALChecksumDurationNanos = storageProfile.WALChecksumDurationNanos
		writeProfile.WALAppendOps = storageProfile.WALAppendOps
		writeProfile.WALAppendBytes = storageProfile.WALAppendBytes
		writeProfile.WALAppendDurationNanos = storageProfile.WALAppendDurationNanos
		writeProfile.WALAppendLockWaitOps = storageProfile.WALAppendLockWaitOps
		writeProfile.WALAppendLockWaitNanos = storageProfile.WALAppendLockWaitNanos
		writeProfile.WriteCommitLockWaitOps = storageProfile.WriteCommitLockWaitOps
		writeProfile.WriteCommitLockWaitNanos = storageProfile.WriteCommitLockWaitNanos
		writeProfile.WALAppendWriteAtCalls = storageProfile.WALAppendWriteAtCalls
		writeProfile.WALAppendWriteAtBytes = storageProfile.WALAppendWriteAtBytes
		writeProfile.WALAppendWriteAtMaxBytes = storageProfile.WALAppendWriteAtMaxBytes
		writeProfile.WALAppendWrapCount = storageProfile.WALAppendWrapCount
		writeProfile.WALAppendPaddingBytes = storageProfile.WALAppendPaddingBytes
		writeProfile.DirtyMapUpdateOps = storageProfile.DirtyMapUpdateOps
		writeProfile.DirtyMapUpdateDurationNanos = storageProfile.DirtyMapUpdateDurationNanos
	}
	return VolumeStatus{
		Warnings:              warnings,
		HolesSuspected:        holes,
		VolumeID:              volumeID,
		Path:                  path,
		Impl:                  impl,
		ReplicaID:             id.ReplicaID,
		Epoch:                 id.Epoch,
		EndpointVersion:       id.EndpointVersion,
		Latched:               id.Epoch > 0 && id.EndpointVersion > 0,
		Operational:           b.operational.Load(),
		FrontierKnown:         true,
		DurableLSN:            durableLSN,
		RetainedLSN:           retainedLSN,
		HeadLSN:               headLSN,
		Evidence:              evidence,
		Closed:                closed,
		WriteProfile:          writeProfile,
		CheckpointPersistence: checkpoint,
		DataSync:              dataSync,
	}
}

// Close marks the backend closed. All subsequent I/O + Sync
// return ErrBackendClosed. Does NOT close the underlying
// storage — the Provider owns LogicalStorage lifecycle.
func (b *StorageBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.finishPrecopyLocked()
	b.notifyReadReadyLocked()
	return nil
}

// SetWriteObserver installs the per-write notification seam used by
// the replication layer. Safe to call only during setup (before
// I/O begins) — the seam is intentionally not hot-swappable so a
// well-behaved caller cannot race replication wiring against live
// writes. Pass nil to disable replication fan-out on this backend.
//
// Called by: Provider (T3b) / Host (T4a-5) during volume wiring,
// once per backend lifetime.
// Owns: observer pointer under b.mu.
// Borrows: the observer — caller retains ownership; StorageBackend
// does not close or otherwise tear down the observer.
//
// An observer that orders writes by LSN (ShipLSNRebaser) is first rebased
// to the store's next LSN: the frontier may have been seeded before the
// install. If that rebase is refused, the observer is NOT installed, the
// error is returned, and the backend refuses all I/O until an install
// succeeds: a volume never serves writes its observer would mis-order.
func (b *StorageBackend) SetWriteObserver(obs WriteObserver) error {
	b.observerMu.Lock()
	defer b.observerMu.Unlock()
	if r, ok := obs.(ShipLSNRebaser); ok {
		if err := r.RebaseShipLSN(b.storage.NextLSN()); err != nil {
			err = fmt.Errorf("durable: write observer not installed: ship LSN: %w", err)
			b.mu.Lock()
			b.observerErr = err
			b.notifyReadReadyLocked()
			b.mu.Unlock()
			return err
		}
	}
	b.mu.Lock()
	b.observer, b.observerErr = obs, nil
	b.notifyReadReadyLocked()
	b.mu.Unlock()
	return nil
}

// seedAndRebase moves the store's next LSN with seed and, in the same
// critical section as observer installs, rebases the installed observer
// to it. A refused rebase also closes I/O (as a refused install does).
func (b *StorageBackend) seedAndRebase(seed func() error) error {
	b.observerMu.Lock()
	defer b.observerMu.Unlock()
	if err := seed(); err != nil {
		return err
	}
	b.mu.Lock()
	obs := b.observer
	b.mu.Unlock()
	r, ok := obs.(ShipLSNRebaser)
	if !ok {
		return nil
	}
	if err := r.RebaseShipLSN(b.storage.NextLSN()); err != nil {
		err = fmt.Errorf("durable: write observer ship LSN after the frontier seed: %w", err)
		b.mu.Lock()
		b.observerErr = err
		b.notifyReadReadyLocked()
		b.mu.Unlock()
		return err
	}
	return nil
}

// SetWriteAckPolicy configures whether observer acknowledgement participates
// in foreground Write success. Default is WriteAckBestEffort.
func (b *StorageBackend) SetWriteAckPolicy(policy WriteAckPolicy) {
	b.mu.Lock()
	b.writeAck = policy
	b.mu.Unlock()
}

// WriteProfile returns a read-only snapshot of the adapter's existing
// cumulative write counters. It does not enable instrumentation or alter the
// write path.
func (b *StorageBackend) WriteProfile() WriteProfileStatus {
	s := b.profile.snapshot()
	if source, ok := b.storage.(interface {
		DurabilityLatency() storage.DurabilityLatencyStatus
	}); ok {
		v := source.DurabilityLatency()
		s.DurabilityLatency.WAL = &v
	}
	return s
}

// SetOperational flips the readiness gate. evidence is surfaced
// in error strings and diagnostic logs — pass a short reason like
// "recovered LSN=123 epoch=4" or "superblock ahead of assignment".
// Safe to call from any goroutine.
//
// Invariant: does NOT touch identity/lineage/epoch — see package
// godoc + INV-DURABLE-OPGATE-001.
func (b *StorageBackend) SetOperational(ok bool, evidence string) {
	if evidence == "" {
		if ok {
			evidence = "operational"
		} else {
			evidence = "not operational"
		}
	}
	b.mu.Lock()
	b.setOperationalLocked(ok, evidence)
	b.mu.Unlock()
}

// The caller owns mu; publication never calls the store or waits for I/O.
func (b *StorageBackend) setOperationalLocked(ok bool, evidence string) {
	flipped := !ok && b.operational.Load()
	b.opEvidence.Store(evidence)
	b.operational.Store(ok)
	if flipped {
		b.stopFlipAt = time.Now() // after the store: no gate after this instant admits
	}
	b.waitAttachReady = false
	// A host drain closes storage admission, but is not the old path's
	// teardown. Keep a final pre-copy queue parked until Close or its budget;
	// publishing readiness loss must not complete commands on that path.
	if ok || b.precopy == nil || !b.precopy.active {
		b.finishPrecopyLocked()
	}
	b.notifyReadReadyLocked()
}

// TryStopIO closes admission only if every admitted I/O has drained already.
// Used under the capture barrier: never wait for ioMu there, since an I/O
// holding its read side may itself be queued on that barrier. Failed attempts
// leave the frontend unchanged. Lock order: try ioMu -> try mu; neither lock waits, and no store calls.
func (b *StorageBackend) TryStopIO(evidence string) bool {
	// A drained admission pause excludes late gate callers even if they still
	// hold ioMu's read side. Their final registration check cannot reach storage.
	if b.mu.TryLock() {
		if b.precopy != nil {
			defer b.mu.Unlock()
			if !b.precopy.active {
				return false
			}
			if b.admittedIO != 0 || b.closed || !b.operational.Load() {
				return false
			}
			b.opEvidence.Store(evidence)
			b.operational.Store(false)
			b.stopFlipAt = time.Now()
			b.waitAttachReady = false
			b.notifyReadReadyLocked()
			return true
		}
		b.mu.Unlock()
	}
	if hook := b.testPrecopyStopFallback; hook != nil {
		hook()
	}
	if !b.ioMu.TryLock() {
		return false
	}
	defer b.ioMu.Unlock()
	if !b.mu.TryLock() {
		return false
	}
	defer b.mu.Unlock()
	// State may have changed after the initial mu attempt. An existing pause
	// owns its waiters and shared clock; refuse rather than replace it.
	if b.precopy != nil || b.closed || !b.operational.Load() {
		return false
	}
	p := &precopyAdmission{active: true, started: time.Now(), drained: make(chan struct{}), drainDone: true}
	close(p.drained)
	b.precopy = p
	b.opEvidence.Store(evidence)
	b.operational.Store(false)
	b.stopFlipAt = time.Now()
	b.waitAttachReady = false
	b.notifyReadReadyLocked()
	return true
}

// stopPlainDetachIfIdle closes admission without waiting for a live command.
// Taking ioMu exclusively also excludes commands between their gate and final
// registration; a failed try leaves every admission/pause field unchanged.
func (b *StorageBackend) stopPlainDetachIfIdle() error {
	if !b.ioMu.TryLock() {
		return ErrPlainDetachBusy
	}
	defer b.ioMu.Unlock()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.admittedIO != 0 {
		return ErrPlainDetachBusy
	}
	b.setOperationalLocked(false, "chunk plain detach: idle frontend stopped")
	return nil
}

// StopFlipAt is when admission last closed for a stop (zero if never).
func (b *StorageBackend) StopFlipAt() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stopFlipAt
}

// StopIO makes the backend refuse every new Read, Write and Sync with
// ErrNotReady (evidence), then waits, bounded by ctx, for the I/O admitted
// before the stop to finish. After a nil return no I/O admitted before the
// stop is still running, so nothing can be acknowledged after a cut taken
// next. On a ctx error I/O stays refused and admitted I/O may still run;
// a later StopIO waits on the same pending drain.
func (b *StorageBackend) StopIO(ctx context.Context, evidence string) error {
	b.SetOperational(false, evidence)
	b.drainMu.Lock()
	drained := b.drain
	if drained == nil {
		drained = make(chan struct{})
		b.drain = drained
		go func() {
			b.ioMu.Lock()
			b.ioMu.Unlock() //nolint:staticcheck // an empty critical section: waiting is the point
			b.drainMu.Lock()
			b.drain = nil
			b.drainMu.Unlock()
			close(drained)
		}()
	}
	b.drainMu.Unlock()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("durable: I/O admitted before the stop is still running: %w", ctx.Err())
	}
}

// Sync flushes everything the storage has to its durability
// boundary. Returns nil iff the stable frontier advanced without
// error. Subject to the same gate stack as I/O: closed → Err-
// BackendClosed, non-operational → ErrNotReady, lineage drift →
// ErrStalePrimary.
func (b *StorageBackend) Sync(ctx context.Context) error {
	iolatency.BackendEnter(ctx)
	defer iolatency.BackendExit(ctx)
	latencyStart := time.Now()
	defer func() { b.profile.syncLatency.Observe(time.Since(latencyStart)) }()
	release, err := b.admitPrecopy(ctx, b.admit)
	if err != nil {
		return err
	}
	defer release()
	start := time.Now()
	// T4b-5: if an observer is installed, delegate the FULL sync
	// job — the observer (ReplicationVolume) owns both the local
	// LogicalStorage.Sync call and the peer barriers fanned out
	// through the peer queues. Without this branch the
	// local sync would run twice (once here, once inside the
	// observer) which is both wasteful and semantically wrong
	// (primary would be "durable" before peers were even queried).
	//
	// targetLSN is the primary's current walHead — the replica-facing
	// intent for what must be durable. Local sync's achieved LSN
	// should meet or exceed this; per-peer barriers carry the
	// session lineage (TargetLSN fixed per session per peer.Barrier
	// godoc).
	b.mu.Lock()
	obs := b.observer
	b.mu.Unlock()
	if obs != nil {
		_, _, walHead := b.storage.Boundaries()
		if err := obs.Sync(ctx, walHead); err != nil {
			return err
		}
		b.profile.recordBackendSync(time.Since(start))
		return nil
	}
	// No observer installed — observer-free path runs local sync only
	// (T0 / bootstrap / single-replica dev configurations).
	if _, err := b.storage.Sync(); err != nil {
		return fmt.Errorf("durable: storage sync: %w", err)
	}
	b.profile.recordBackendSync(time.Since(start))
	return nil
}

// Read copies up to len(p) bytes starting at offset. Reads past
// end-of-volume return zero bytes (no error). Every read is gated.
func (b *StorageBackend) Read(ctx context.Context, offset int64, p []byte) (int, error) {
	ctx, cancel := frontend.WithReadBudget(ctx, time.Time{})
	defer cancel()
	release, err := b.admitAfterAttach(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	if len(p) == 0 {
		return 0, nil
	}
	if ctx.Err() != nil {
		return 0, b.attachReadyTimeout(ctx.Err())
	}
	n, err := b.readBytes(ctx, offset, p)
	if hydrationPendingOnly(err) {
		if gateErr := b.gate(); gateErr != nil {
			return 0, gateErr
		}
		return n, fmt.Errorf("%w: %w", frontend.ErrBackendBusy, err)
	}
	return n, err
}

// setChunkAttachPending is used only after successful local recovery.
func (b *StorageBackend) setChunkAttachPending(evidence string) {
	b.mu.Lock()
	b.opEvidence.Store(evidence)
	b.operational.Store(false)
	b.waitAttachReady = true
	b.notifyReadReadyLocked()
	b.mu.Unlock()
}

func (b *StorageBackend) admitAfterAttach(ctx context.Context) (func(), error) {
	return b.admitPrecopy(ctx, func() (func(), error) { return b.admitAfterAttachUnqueued(ctx) })
}

func (b *StorageBackend) admitAfterAttachUnqueued(ctx context.Context) (func(), error) {
	release, err := b.admit()
	if !errors.Is(err, frontend.ErrNotReady) {
		return release, err
	}
	if hook := b.testAfterAttachRefusal; hook != nil {
		hook()
	}
	b.mu.Lock()
	pending := b.waitAttachReady
	b.mu.Unlock()
	if !pending {
		// Readiness may have published after the initial refusal.
		return b.admit()
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return b.awaitAttachAdmission(ctx)
}

// notifyReadReadyLocked wakes waiting I/O without making them hold ioMu while
// the provider is attaching or draining the backend. Caller holds mu.
func (b *StorageBackend) notifyReadReadyLocked() {
	if b.readyChanged != nil {
		close(b.readyChanged)
		b.readyChanged = nil
	}
}

// awaitAttachAdmission waits only after a gate refusal, before any storage I/O.
// The three-second readiness allowance is capped by an earlier caller deadline.
func (b *StorageBackend) awaitAttachAdmission(ctx context.Context) (func(), error) {
	for {
		b.mu.Lock()
		if b.readyChanged == nil {
			b.readyChanged = make(chan struct{})
		}
		changed := b.readyChanged
		terminal := b.closed || b.observerErr != nil || !b.waitAttachReady
		b.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return nil, b.attachReadyTimeout(err)
		}
		release, err := b.admit()
		if err == nil {
			if cause := ctx.Err(); cause != nil {
				release()
				return nil, b.attachReadyTimeout(cause)
			}
			return release, nil
		}
		if !errors.Is(err, frontend.ErrNotReady) || terminal || b.operational.Load() {
			return release, err
		}
		if hook := b.testBeforeReadReadyWait; hook != nil {
			hook()
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, b.attachReadyTimeout(ctx.Err())
		}
	}
}

func (b *StorageBackend) attachReadyTimeout(cause error) error {
	// A concurrent Close, fence or observer failure must retain its gate error.
	err := b.gate()
	b.mu.Lock()
	observerFailed := b.observerErr != nil
	b.mu.Unlock()
	if err != nil && (!errors.Is(err, frontend.ErrNotReady) || observerFailed) {
		return err
	}
	return fmt.Errorf("%w: %w: chunk attachment readiness: %w", frontend.ErrNotReady, frontend.ErrBackendBusy, cause)
}

// Write persists up to len(p) bytes at offset. Partial-block
// writes perform a read-modify-write: fetch the current block,
// overlay the partial range, write the full block back.
// Every write is gated.
func (b *StorageBackend) Write(ctx context.Context, offset int64, p []byte) (int, error) {
	ctx, cancel := frontend.WithWriteBudget(ctx, time.Time{})
	defer cancel()
	iolatency.BackendEnter(ctx)
	defer iolatency.BackendExit(ctx)
	release, err := b.admitAfterAttach(ctx)
	if err != nil {
		log.Printf("durable: StorageBackend.Write gate-rejected offset=%d: %v", offset, err)
		return 0, err
	}
	defer release()
	if len(p) == 0 {
		return 0, nil
	}
	if ctx.Err() != nil {
		return 0, b.attachReadyTimeout(ctx.Err())
	}
	b.profile.recordBackendWriteRequest(len(p))
	start := time.Now()
	n, err := b.writeBytes(ctx, offset, p)
	if hydrationPendingOnly(err) {
		if gateErr := b.gate(); gateErr != nil {
			err = gateErr
		}
	}
	if errors.Is(err, storage.ErrWALPressureTimeout) || hydrationPendingOnly(err) {
		err = fmt.Errorf("%w: %w", frontend.ErrBackendBusy, err)
	}
	if n > 0 {
		b.profile.recordBackendWrite(n, time.Since(start))
	}
	return n, err
}

func (b *StorageBackend) BeginDurabilityRequest(ctx context.Context, kind string) (context.Context, *iolatency.Request) {
	return b.profile.requests.Begin(ctx, kind)
}

// RecordTargetWrite records a successful frontend-target write that reached
// this durable backend. NVMe/TCP uses this to expose target-level write-path
// evidence without importing durable internals into the transport package.
func (b *StorageBackend) RecordTargetWrite(bytes int, d time.Duration) {
	b.profile.recordTargetWrite(bytes, d)
}

// admit admits one I/O: it passes the gate under the read side of ioMu
// and returns the release. A first check of the closed and operational
// flags before the lock makes I/O that arrives while StopIO drains fail at
// once rather than wait; the full gate runs under the lock.
func (b *StorageBackend) admit() (func(), error) {
	if err := b.readyCheck(); err != nil {
		return nil, err
	}
	if hook := b.testBeforeIOLock; hook != nil {
		hook()
	}
	b.ioMu.RLock()
	if err := b.gate(); err != nil {
		b.ioMu.RUnlock()
		return nil, err
	}
	if hook := b.testAfterGate; hook != nil {
		hook()
	}
	b.mu.Lock()
	if b.precopy != nil && b.precopy.active {
		b.mu.Unlock()
		b.ioMu.RUnlock()
		return nil, errPrecopyPaused
	}
	b.admittedIO++
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		b.admittedIO--
		if p := b.precopy; p != nil && p.active && b.admittedIO == 0 && !p.drainDone {
			close(p.drained)
			p.drainDone = true
		}
		b.mu.Unlock()
		b.ioMu.RUnlock()
	}, nil
}

// gate is the per-I/O precondition stack. Order matters:
//
//  1. closed: tests the lifecycle, cheapest check
//  2. operational: tests local readiness; preserves Add.A Add.1
//  3. lineage: tests that the captured Identity still matches
//     the live projection; preserves INV-FRONTEND-002
//
// Returning ANY error short-circuits — no partial I/O.
func (b *StorageBackend) gate() error {
	if err := b.readyCheck(); err != nil {
		return err
	}
	return b.lineageCheck()
}

// readyCheck is the gate without the lineage check: closed, then
// operational.
func (b *StorageBackend) readyCheck() error {
	b.mu.Lock()
	closed := b.closed
	chunkFenced := b.chunkFenced
	observerErr := b.observerErr
	precopyPending := b.precopy != nil && b.precopy.active
	b.mu.Unlock()
	if closed {
		return frontend.ErrBackendClosed
	}
	if observerErr != nil {
		return fmt.Errorf("%w: %v", frontend.ErrNotReady, observerErr)
	}
	if chunkFenced != nil && chunkFenced() {
		return frontend.ErrStalePrimary
	}
	if precopyPending {
		return errPrecopyPaused
	}
	if !b.operational.Load() {
		ev, _ := b.opEvidence.Load().(string)
		return fmt.Errorf("%w: %s", frontend.ErrNotReady, ev)
	}
	return nil
}

// lineageCheck re-reads the projection and compares all four
// facets plus Healthy. Any drift → ErrStalePrimary.
// INV-FRONTEND-002.{EPOCH,EV,REPLICA,HEALTHY}.
func (b *StorageBackend) lineageCheck() error {
	proj := b.view.Projection()
	if !proj.Healthy ||
		proj.VolumeID != b.id.VolumeID ||
		proj.ReplicaID != b.id.ReplicaID ||
		proj.Epoch != b.id.Epoch ||
		proj.EndpointVersion != b.id.EndpointVersion {
		return frontend.ErrStalePrimary
	}
	return nil
}

// readStorageBlock shares the request deadline across blocks and partial-write
// RMW. Legacy engines may still perform contextless local I/O (#221).
func (b *StorageBackend) readStorageBlock(ctx context.Context, lba uint32) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: %w", storage.ErrChunkHydrationPending, err)
	}
	var data []byte
	var err error
	if r, ok := b.storage.(storage.ContextReader); ok {
		data, err = r.ReadContext(ctx, lba)
	} else {
		data, err = b.storage.Read(lba)
	}
	if err != nil {
		return nil, err
	} // a known substrate failure wins expiry
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: %w", storage.ErrChunkHydrationPending, err)
	}
	return data, nil
}

func hydrationPendingOnly(err error) bool {
	return errors.Is(err, storage.ErrChunkHydrationPending) && hydrationPendingCauseOnly(err)
}

func hydrationPendingCauseOnly(err error) bool {
	if err == storage.ErrChunkHydrationPending || err == context.DeadlineExceeded || err == context.Canceled {
		return true
	}
	if err == nil {
		return false
	}
	if j, ok := err.(interface{ Unwrap() []error }); ok {
		causes := j.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !hydrationPendingCauseOnly(cause) {
				return false
			}
		}
		return true
	}
	return hydrationPendingCauseOnly(errors.Unwrap(err))
}

// readBytes performs the byte-range → LBA-range fanout for Read.
// Reads block-at-a-time, copying the sub-slice covered by the
// current byte range. Handles three cases:
//
//   - offset aligned, len >= block: copy full block, advance.
//   - offset misaligned: copy tail of first block only.
//   - final block, len < available: copy prefix only.
func (b *StorageBackend) readBytes(ctx context.Context, offset int64, p []byte) (int, error) {
	if offset < 0 {
		return 0, errInvalidOffset
	}
	bs := int64(b.blockSize)
	total := 0
	buf := p
	pos := offset
	for len(buf) > 0 {
		lba := uint32(pos / bs)
		block, err := b.readStorageBlock(ctx, lba)
		if err != nil {
			return total, fmt.Errorf("durable: read lba=%d: %w", lba, err)
		}
		inBlockOff := int(pos % bs)
		if inBlockOff >= len(block) {
			// Short block (shouldn't happen with fixed-size storage)
			// — treat as zeros, advance.
			advance := int(bs) - inBlockOff
			if advance > len(buf) {
				advance = len(buf)
			}
			for i := 0; i < advance; i++ {
				buf[i] = 0
			}
			total += advance
			buf = buf[advance:]
			pos += int64(advance)
			continue
		}
		copied := copy(buf, block[inBlockOff:])
		total += copied
		buf = buf[copied:]
		pos += int64(copied)
	}
	return total, nil
}

// writeBytes performs the byte-range → LBA-range fanout for Write.
// Full-block writes skip the read (optimization for the hot path);
// partial-block writes read-modify-write to preserve the bytes
// outside the range.
func (b *StorageBackend) writeBytes(ctx context.Context, offset int64, p []byte) (int, error) {
	if offset < 0 {
		return 0, errInvalidOffset
	}
	bs := int64(b.blockSize)
	total := 0
	buf := p
	pos := offset
	for len(buf) > 0 {
		lba := uint32(pos / bs)
		inBlockOff := int(pos % bs)
		b.mu.Lock()
		obs := b.observer
		policy := b.writeAck
		b.mu.Unlock()
		if _, ok := b.storage.(storage.WriteBatcher); ok && inBlockOff == 0 && len(buf) >= int(bs) && canBatchFullBlockRun(policy, obs) {
			fullBlocks := len(buf) / int(bs)
			if fullBlocks > maxFullBlockBatchBlocks {
				fullBlocks = maxFullBlockBatchBlocks
			}
			if fullBlocks > 1 {
				written, err := b.writeFullBlockBatch(ctx, lba, buf[:fullBlocks*int(bs)], obs, policy)
				total += written
				buf = buf[written:]
				pos += int64(written)
				if err != nil {
					return total, err
				}
				continue
			}
		}
		available := int(bs) - inBlockOff
		chunkLen := len(buf)
		if chunkLen > available {
			chunkLen = available
		}

		writeLock := &b.lbaWriteLocks[lba%uint32(len(b.lbaWriteLocks))]
		writeLock.Lock()
		var block []byte
		if inBlockOff == 0 && chunkLen == int(bs) {
			// Full-block write: skip the read.
			block = buf[:chunkLen]
		} else {
			// Partial-block: RMW.
			existing, err := b.readStorageBlock(ctx, lba)
			if err != nil {
				writeLock.Unlock()
				return total, fmt.Errorf("durable: RMW read lba=%d: %w", lba, err)
			}
			block = make([]byte, bs)
			copy(block, existing)
			copy(block[inBlockOff:], buf[:chunkLen])
		}
		if obs == nil && policy == WriteAckRequireObserverAck {
			writeLock.Unlock()
			return total, fmt.Errorf("%w: no write observer installed", ErrReplicationAckUnavailable)
		}

		var lsn uint64
		var err error
		if writer, ok := b.storage.(storage.ContextWriter); ok {
			lsn, err = writer.WriteContext(ctx, lba, block)
		} else if err = expiredCommand(ctx); err == nil {
			lsn, err = b.storage.Write(lba, block)
		}
		writeLock.Unlock()
		if err != nil {
			return total, fmt.Errorf("durable: write lba=%d: %w", lba, err)
		}
		b.profile.recordBackendStorageWrite(1, false)
		if err := b.observeWrite(ctx, lba, lsn, block, obs, policy); err != nil {
			return total + chunkLen, err
		}
		total += chunkLen
		buf = buf[chunkLen:]
		pos += int64(chunkLen)
	}
	return total, nil
}

func canBatchFullBlockRun(policy WriteAckPolicy, obs WriteObserver) bool {
	if policy != WriteAckRequireObserverAck {
		return true
	}
	_, ok := obs.(BatchWriteObserver)
	return ok
}

func (b *StorageBackend) writeFullBlockBatch(ctx context.Context, startLBA uint32, p []byte, obs WriteObserver, policy WriteAckPolicy) (int, error) {
	batcher, ok := b.storage.(storage.WriteBatcher)
	if !ok {
		return 0, fmt.Errorf("durable: write batch unsupported")
	}
	bs := b.blockSize
	blockCount := len(p) / bs
	blocks := make([][]byte, blockCount)
	for i := range blocks {
		blocks[i] = p[i*bs : (i+1)*bs]
	}
	unlock := b.lockLBARange(startLBA, blockCount)
	var lsns []uint64
	var err error
	if writer, ok := batcher.(storage.ContextWriteBatcher); ok {
		lsns, err = writer.WriteBatchContext(ctx, startLBA, blocks)
	} else if err = expiredCommand(ctx); err == nil {
		lsns, err = batcher.WriteBatch(startLBA, blocks)
	}
	unlock()
	if len(lsns) > 0 {
		b.profile.recordBackendStorageWrite(len(lsns), true)
		b.profile.recordBackendFullBlockBatch(len(lsns))
	}
	if batchObserver, ok := obs.(BatchWriteObserver); ok && len(lsns) > 0 {
		if obsErr := batchObserver.ObserveBatch(ctx, startLBA, lsns, blocks[:len(lsns)]); obsErr != nil {
			log.Printf("durable: replication batch fan-out failed lba=%d blocks=%d: %v",
				startLBA, len(lsns), obsErr)
			if policy == WriteAckRequireObserverAck {
				return len(lsns) * bs, replicationWriteError(ctx, withStorageCause(startLBA, blockCount, err, obsErr))
			}
		}
	} else {
		for i, lsn := range lsns {
			block := blocks[i]
			lba := startLBA + uint32(i)
			if obsErr := b.observeWriteCause(ctx, lba, lsn, block, obs, policy, withStorageCause(startLBA, blockCount, err, nil)); obsErr != nil {
				return (i + 1) * bs, obsErr
			}
		}
	}
	if err != nil {
		return len(lsns) * bs, fmt.Errorf("durable: write batch lba=%d blocks=%d: %w", startLBA, blockCount, err)
	}
	if len(lsns) != blockCount {
		return len(lsns) * bs, fmt.Errorf("durable: write batch lba=%d returned %d LSNs for %d blocks", startLBA, len(lsns), blockCount)
	}
	return len(p), nil
}

func (b *StorageBackend) lockLBARange(startLBA uint32, blockCount int) func() {
	var used [256]bool
	indices := make([]int, 0, blockCount)
	for i := 0; i < blockCount; i++ {
		index := int((startLBA + uint32(i)) % uint32(len(b.lbaWriteLocks)))
		if !used[index] {
			used[index] = true
			indices = append(indices, index)
		}
	}
	sort.Ints(indices)
	for _, index := range indices {
		b.lbaWriteLocks[index].Lock()
	}
	return func() {
		for i := len(indices) - 1; i >= 0; i-- {
			b.lbaWriteLocks[indices[i]].Unlock()
		}
	}
}

func (b *StorageBackend) observeWrite(ctx context.Context, lba uint32, lsn uint64, block []byte, obs WriteObserver, policy WriteAckPolicy) error {
	return b.observeWriteCause(ctx, lba, lsn, block, obs, policy, nil)
}

// observeWriteCause is observeWrite with the storage failure of the same
// command (nil if none): a required observer's error is classified together
// with it, so a physical storage fault is never reclassified as retryable.
func (b *StorageBackend) observeWriteCause(ctx context.Context, lba uint32, lsn uint64, block []byte, obs WriteObserver, policy WriteAckPolicy, storageErr error) error {
	// Replication fan-out (T4a-6 hook). Error is logged best-effort
	// unless the ack policy requires observer acknowledgement.
	if obs == nil {
		return nil
	}
	if werr := obs.Observe(ctx, lba, lsn, block); werr != nil {
		log.Printf("durable: replication fan-out failed lba=%d lsn=%d: %v",
			lba, lsn, werr)
		if policy == WriteAckRequireObserverAck {
			if storageErr != nil {
				werr = errors.Join(storageErr, werr)
			}
			return replicationWriteError(ctx, werr)
		}
	}
	return nil
}

// withStorageCause joins a batch's storage error (nil if none) with an
// observer error, so the classifier sees the complete error set (#252):
// a committed prefix may come back together with a physical failure.
func withStorageCause(startLBA uint32, blockCount int, storageErr, obsErr error) error {
	if storageErr == nil {
		return obsErr
	}
	se := fmt.Errorf("durable: write batch lba=%d blocks=%d: %w", startLBA, blockCount, storageErr)
	if obsErr == nil {
		return se
	}
	return errors.Join(se, obsErr)
}

// Classify the observer cause before adding the acknowledgement sentinel;
// otherwise that sentinel would obscure a pure caller deadline at the target.
func replicationWriteError(ctx context.Context, err error) error {
	wrapped := fmt.Errorf("%w: %w", ErrReplicationAckUnavailable, err)
	if frontend.IsWriteDeadline(ctx, err) {
		return fmt.Errorf("%w: %w", frontend.ErrBackendBusy, wrapped)
	}
	return wrapped
}

// Compile-time check that StorageBackend satisfies frontend.Backend.
// Catches interface drift at build time rather than runtime.
var _ frontend.Backend = (*StorageBackend)(nil)

// expiredCommand refuses a fresh mutation through a contextless storage
// fallback once the command is already expired or cancelled (#252); a pure
// deadline is retryable busy, as on the context-aware path.
func expiredCommand(ctx context.Context) error {
	err := ctx.Err()
	if err == nil {
		return nil
	}
	if frontend.IsWriteDeadline(ctx, err) {
		return fmt.Errorf("%w: %w", frontend.ErrBackendBusy, err)
	}
	return err
}
