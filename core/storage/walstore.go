package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweed-block/core/geometry"
	"github.com/seaweedfs/seaweed-block/core/iolatency"
)

// WALStore is a crash-safe block store backed by one preallocated
// file with three regions: superblock, circular WAL, and a
// block-indexed extent. Writes append to the WAL and become durable
// at the next group-committed fsync; an explicit Flush moves
// committed WAL entries into their extent slots and advances the
// checkpoint LSN, so the WAL stays bounded across long write streams.
//
// File layout:
//
//	[0 .. superblockSize)               4KB superblock (magic, geometry, WAL state)
//	[superblockSize .. extentOffset)    circular WAL region (sb.WALSize bytes)
//	[extentOffset .. extentOffset+VolSz) block-indexed extent (sb.VolumeSize bytes)
//
// Concurrency model: a single Mutex guards in-memory metadata; the
// WAL writer has its own internal mutex; reads of dirty entries take
// only that smaller lock. Writes are durable when Sync() returns.
//
// What this implementation provides (see core/storage/logical_storage.go
// for the full LogicalStorage contract):
//   - Acked writes (covered by a returned Sync) survive process kill
//     and clean restart.
//   - Recovery is deterministic: replay WAL entries past the last
//     checkpoint into the dirty map; reads find them there until
//     Flush moves them to the extent.
//   - Group commit batches concurrent Sync() callers into one fsync.
//
// What this implementation deliberately does NOT do (preserve narrow
// scope; future implementations behind LogicalStorage may add):
//   - Snapshots
//   - Replication-aware ship/apply paths
//   - Online resize
//   - Compaction (the WAL is bounded but periodic checkpoints free space)
type WALStore struct {
	// recoveryWarnings and holesSuspected: the last Recover's typed
	// warnings, and how many recoveries found a WAL hole (issue #51).
	recoveryWarnings []string
	holesSuspected   uint64

	fileSyncLatency iolatency.Histogram
	metadata        BlockMetadata // immutable before publication

	path                       string
	fd                         *os.File
	sb                         *superblock
	wal                        *walWriter
	dm                         *dirtyMap
	committer                  *GroupCommitter
	syncCache                  func() error
	syncDirectFrontierMetadata func([]byte) error
	writeSuperblockMetadata    func([]byte) error
	syncSuperblockMetadata     func() error
	flusher                    *flusher      // background WAL→extent applier; nil if disabled
	admission                  *walAdmission // backpressure on WAL pressure; nil if disabled

	// admissionTimeout is the per-Write deadline for admission. If
	// the WAL stays above the hard watermark for longer than this,
	// the Write returns errWALFull. Defaults to 30s.
	admissionTimeout time.Duration

	// Publication lock order: lifecycleMu -> mu -> WAL mutex, then dirty
	// shard (WAL and shard are not held together). Snapshot takes mu ->
	// shard. Never acquire mu while holding a dirty-shard lock.
	lifecycleMu sync.RWMutex
	mu          sync.RWMutex
	// captureMu is the chunk layer's capture write barrier: Write,
	// WriteBatch, ApplyEntry, WriteExtentDirect and Recover hold it shared
	// (before lifecycleMu); a quiesced cut holds it exclusively. Order:
	// captureMu -> lifecycleMu -> mu.
	captureMu sync.RWMutex
	// captureMu orders permanent attachment shutdown with hook publication.
	attachClosed            bool
	closeComplete           chan struct{}
	closeResult             error
	testDestructiveSnapshot func() // nil scheduling control, before any capture lock
	testCloseSnapshot       func() // nil in production; called without captureMu

	// chunk is the attached chunk layer's hooks; nil when none.
	chunk     atomic.Pointer[chunkHooks]
	extentMu  [64]sync.RWMutex
	closed    bool
	nextLSN   uint64 // next LSN to assign to a write
	syncedLSN uint64 // highest LSN durably present in extent OR WAL after fsync
	walTail   uint64 // exposed S boundary (oldest retained LSN)
	walHead   uint64 // exposed H boundary (newest written LSN)

	// extentBase is the absolute file offset where the extent region
	// begins; cached for fast read/write.
	extentBase uint64

	// checkpointLSN is the highest LSN whose data has been durably
	// written into the extent and recorded in the on-disk superblock.
	// Recovery skips WAL records with LSN <= checkpointLSN. Advanced
	// only by the flusher.
	checkpointLSN uint64

	// pendingDirectFrontierLSN is set by AdvanceFrontier after direct
	// extent writes (rebuild BASE lane). It is persisted as checkpoint
	// metadata only after a successful Sync, so an un-synced BASE install
	// cannot be mistaken for durable recovery progress after a crash.
	pendingDirectFrontierLSN uint64

	// recoveryRetentionLSNs is the operator-tunable retention window
	// past checkpointLSN: the recovery scan accepts fromLSN as long
	// as fromLSN > checkpointLSN - recoveryRetentionLSNs. Zero means
	// strict checkpoint-driven recycle. Operator sets via
	// blockvolume's --wal-retention-lsns flag at store construction.
	// Stored in-memory only — NOT persisted to the superblock; on
	// restart the daemon re-applies the flag.
	//
	// Pinned by: INV-G6-RETENTION-POLICY-OPERATOR-VISIBLE.
	recoveryRetentionLSNs uint64

	// recycleFloorSrc gates `persistCheckpoint` advancement when
	// non-nil: the proposed checkpoint cannot exceed the source's
	// reported `MinPinAcrossActiveSessions`. Set via
	// SetRecycleFloorSource at daemon wiring time when
	// --recovery-mode=dual-lane.
	//
	// nil = no gate (legacy behavior). Per docs/recovery-wiring-plan.md
	// §6 + INV-RECYCLE-GATED-BY-MIN-ACTIVE-PIN.
	recycleFloorSrc RecycleFloorSource

	syncs atomic.Uint64 // completed logical Sync calls (test/diagnostic)
	instr writeInstrumentation

	// multiBlockRecords is a disabled-by-default Phase 148 local prototype.
	// It must not be wired into production paths before version/compatibility
	// gates and mounted NVMe/TCP profiling pass.
	multiBlockRecords bool

	// v3 is non-nil for a format-v3 store (walstore_v3.go). Production
	// selectors use v3; legacy formats require explicit offline migration.
	v3 *walStoreV3State

	// Physical WAL sync attempts exclude the separate metadata/checkpoint syncs.
	syncWALFile  func() error
	walSyncCalls atomic.Uint64
	// syncExtentFile is the flusher's fsync after extent writes (the same
	// file as the WAL); a seam for fault injection.
	syncExtentFile func() error
	// ioFault latches the first fsync failure of the file (ErrFsyncFailed)
	// or the first failed WAL append (ErrAppendFailed).
	ioFault atomic.Pointer[RecoveryFailure]
	// fileSyncMu serializes every fsync of the store's file (WAL, extent,
	// superblock) with the latching of its failure, and each superblock
	// publication holds it across its pwrite and fsync: a publication that
	// passes the fault check cannot complete after another path's fsync
	// failed. Leaf lock: taken last, nothing is acquired under it.
	fileSyncMu sync.Mutex
	// testAfterAdmit, when set, runs in Write, WriteBatch and ApplyEntry
	// after their entry checks and before the append lock. Tests only.
	testAfterAdmit func()
	// testBeforeFileSync, when set, runs in syncFile just before fileSyncMu
	// is taken. Tests only.
	testBeforeFileSync func()
	// Generations cover completed append attempts and recovery publication.
	walMutation     atomic.Uint64
	durableMutation atomic.Uint64
	// Replicated/direct paths conservatively disable dedup for this open store.
	bypassSyncDedup atomic.Bool
}

// CreateWALStore initializes a new store file at path. Fails if path
// already exists. Use OpenWALStore for the recovery path.
//
// numBlocks defines the addressable block range; blockSize is the IO
// unit (default 4096 if zero). The on-disk file is preallocated to
// hold the superblock + WAL region + a fully addressable extent.
func CreateWALStore(path string, numBlocks uint32, blockSize int) (*WALStore, error) {
	return CreateWALStoreWithOptions(path, numBlocks, blockSize, WALStoreOptions{})
}

func CreateWALStoreWithOptions(path string, numBlocks uint32, blockSize int, opts WALStoreOptions) (*WALStore, error) {
	if blockSize == 0 {
		blockSize = DefaultBlockSize
	}
	if blockSize < 0 || uint64(blockSize) > uint64(^uint32(0)) {
		return nil, errSuperblockBadVolume
	}
	if err := geometry.ValidateVolumeSize(uint64(numBlocks) * uint64(blockSize)); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	cb := opts.ChunkBlocks
	if cb == 0 {
		cb = 1024
	}
	return createWALStoreV3Configured(path, numBlocks, uint32(blockSize), 64*1024*1024, opts.WMode, 0, uint64(cb), opts)
}

// Frozen constructor for legacy-format regression fixtures; public Create
// never reaches it after activation. Removed from production in row 14a.
func createLegacyWALStoreWithOptions(path string, numBlocks uint32, blockSize int, opts WALStoreOptions) (*WALStore, error) {
	if blockSize == 0 {
		blockSize = DefaultBlockSize
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("storage: mkdir %s: %w", filepath.Dir(path), err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: create %s: %w", path, err)
	}

	volumeBytes := uint64(numBlocks) * uint64(blockSize)
	sb, err := newSuperblock(volumeBytes, createOptions{
		BlockSize:   uint32(blockSize),
		ExtentSize:  uint32(blockSize), // one block per extent slot keeps math trivial
		ImplKind:    ImplKindWALStore,
		ImplVersion: 1,
	})
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	sb.CreatedAt = uint64(time.Now().UnixNano())

	metadata, totalSize, err := bindBlockMetadata(f, &sb, opts, true, false)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := f.Truncate(totalSize); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("storage: preallocate %d bytes: %w", totalSize, err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("storage: seek to header: %w", err)
	}
	if _, err := sb.writeTo(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("storage: write header: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("storage: fsync after create: %w", err)
	}
	return openInitializedWithMetadata(path, f, &sb, metadata)
}

// OpenWALStore opens an existing store file WITHOUT recovering it.
// Call Recover() before any Read/Write to replay the WAL.
func OpenWALStore(path string) (*WALStore, error) {
	return OpenWALStoreWithOptions(path, WALStoreOptions{})
}

func OpenWALStoreWithOptions(path string, opts WALStoreOptions) (*WALStore, error) {
	return openWALStoreV3WithOptions(path, true, opts)
}

func openLegacyWALStoreWithOptions(path string, opts WALStoreOptions) (*WALStore, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", path, err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("storage: seek to header: %w", err)
	}
	sb, err := readSuperblock(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := sb.validate(); err != nil {
		_ = f.Close()
		return nil, err
	}
	metadata, _, err := bindBlockMetadata(f, &sb, opts, false, false)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return openInitializedWithMetadata(path, f, &sb, metadata)
}

// openInitialized is the shared constructor body used by both Create
// and Open after the superblock is in place. It wires the WAL writer,
// the dirty map, and the group committer; it does NOT replay the WAL
// (that is Recover's job).
func openInitialized(path string, f *os.File, sb *superblock) (*WALStore, error) {
	return openInitializedWithMetadata(path, f, sb, nil)
}

func openInitializedWithMetadata(path string, f *os.File, sb *superblock, metadata BlockMetadata) (*WALStore, error) {
	return openInitializedInternal(path, f, sb, metadata, nil)
}

// openInitializedInternal is openInitializedWithMetadata with an optional
// init hook that runs before the committer and flusher goroutines start
// (the v3 store attaches its state there).
func openInitializedInternal(path string, f *os.File, sb *superblock, metadata BlockMetadata, init func(*WALStore) error) (*WALStore, error) {
	dm := newDirtyMap(64) // 64 shards is plenty for Phase 07 demo workloads

	s := &WALStore{
		metadata:      metadata,
		path:          path,
		fd:            f,
		sb:            sb,
		dm:            dm,
		extentBase:    sb.WALOffset + sb.WALSize,
		nextLSN:       sb.WALCheckpointLSN + 1,
		syncedLSN:     sb.WALCheckpointLSN,
		walTail:       retainedLSNFromCheckpoint(sb.WALCheckpointLSN),
		walHead:       sb.WALCheckpointLSN,
		checkpointLSN: sb.WALCheckpointLSN,
	}
	wal := newWALWriter(f, sb.WALOffset, sb.WALSize, sb.WALHead, sb.WALTail, &s.instr)
	s.wal = wal
	if s.nextLSN < 1 {
		s.nextLSN = 1
	}
	// The initial superblock must be synced even before the first write.
	s.walMutation.Store(1)
	s.syncWALFile = func() error {
		s.walSyncCalls.Add(1)
		return f.Sync()
	}
	s.syncExtentFile = f.Sync
	if init != nil {
		if err := init(s); err != nil {
			return nil, err
		}
	}
	committer := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: s.syncWALBatch,
		MaxDelay: 1 * time.Millisecond,
		MaxBatch: 64,
	})
	go committer.Run()
	s.committer = committer
	s.syncCache = committer.SyncCache
	// The two legacy superblock writers pwrite offset 0, which on a v3 file
	// is descriptor slot A: they refuse on v3 (codex01 row 7 slice 4 P1).
	// A v3 store publishes metadata only through its descriptor publisher.
	s.syncDirectFrontierMetadata = func(data []byte) error {
		if s.v3 != nil {
			return errV3LegacySuperblockWrite
		}
		if _, err := f.WriteAt(data, 0); err != nil {
			return fmt.Errorf("storage: pwrite direct frontier superblock: %w", err)
		}
		if err := f.Sync(); err != nil {
			return fmt.Errorf("storage: fsync direct frontier superblock: %w", err)
		}
		return nil
	}
	s.writeSuperblockMetadata = func(data []byte) error {
		if s.v3 != nil {
			return errV3LegacySuperblockWrite
		}
		if _, err := f.WriteAt(data, 0); err != nil {
			return err
		}
		return nil
	}
	s.syncSuperblockMetadata = f.Sync

	// Background flusher: drains dirty map → extent, advances
	// checkpoint, allows WAL recycling. Defaults are conservative
	// for the demo workload; tunable via config in future.
	s.flusher = newFlusher(s, flusherConfig{})
	go s.flusher.run()

	// WAL admission: backpressure when WAL is full. Soft watermark
	// throttles writers slightly; hard watermark blocks them until
	// the flusher drains. Both wake the flusher via Notify(). This
	// is the V2-faithful way to handle WAL pressure without
	// returning ErrWALFull to callers under transient load.
	// Watermarks 0.7/0.9 match V2 config.go defaults
	// (WALSoftWatermark, WALHardWatermark). Admission wakes the
	// flusher via NotifyUrgent — same seam V2 wires for pressure-
	// driven flushes.
	s.admissionTimeout = 30 * time.Second
	if s.v3 != nil {
		// WAL pressure gets a budget below the host's I/O timeout. This bounds
		// cooperative admission waits, not a blocking callback or physical I/O.
		s.admissionTimeout = 4 * time.Second
	}
	s.admission = newWALAdmission(walAdmissionConfig{
		MaxConcurrent: 64,
		SoftWatermark: 0.70,
		HardWatermark: 0.90,
		WALUsedFn:     s.walUsedFraction,
		FaultFn:       s.stickyIOFault,
		NotifyFn: func() {
			if s.flusher != nil {
				s.flusher.NotifyUrgent()
			}
		},
		ClosedFn: func() bool {
			s.mu.RLock()
			defer s.mu.RUnlock()
			return s.closed
		},
	})

	return s, nil
}

// walUsedFraction samples the active format's writer. The legacy writer is
// still present on a v3 store but receives no appends, so its occupancy is zero.
// Store admission is held only for this snapshot, never across a pressure wait.
func (s *WALStore) walUsedFraction() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.v3 == nil {
		return s.wal.usedFraction()
	}
	w := s.v3.w
	if w == nil {
		// Not a capacity decision: append must retain its typed binding refusal
		// without waiting for pressure that cannot be relieved before Recover.
		return 0
	}
	d, floor, valid := s.v3.pub.walSnapshotV3()
	w.mu.Lock()
	defer w.mu.Unlock()
	if !valid || d.WALSize == 0 || d.Nonce != w.nonce || w.head < floor || w.head-floor > d.WALSize {
		return 0 // append's binding validation still refuses; this grants no reuse
	}
	return float64(w.head-floor) / float64(d.WALSize)
}

func retainedLSNFromCheckpoint(checkpointLSN uint64) uint64 {
	if checkpointLSN == 0 {
		return 0
	}
	return checkpointLSN + 1
}

func (s *WALStore) writeExtentIfCurrent(lba, expectedLSN uint64, data []byte) (bool, error) {
	lock := &s.extentMu[lba%uint64(len(s.extentMu))]
	lock.Lock()
	defer lock.Unlock()
	_, _, currentLSN, _, ok := s.dm.get(lba)
	if !ok || currentLSN != expectedLSN {
		return false, nil
	}
	if err := s.writeExtentUnlocked(uint32(lba), data); err != nil {
		return false, err
	}
	if h := s.chunk.Load(); h != nil {
		h.sink.OnExtentWrite(lba, expectedLSN)
	}
	return true, nil
}

func (s *WALStore) writeExtentUnlocked(lba uint32, data []byte) error {
	off := int64(s.extentBase + uint64(lba)*uint64(s.sb.BlockSize))
	if _, err := s.fd.WriteAt(data, off); err != nil {
		return fmt.Errorf("storage: write extent LBA %d: %w", lba, err)
	}
	if s.metadata != nil {
		return s.metadata.OnExtentWrite(lba, data)
	}
	return nil
}

// persistCheckpoint advances the on-disk checkpoint LSN to highestLSN
// (after the flusher has confirmed the extent writes are fsync'd).
// Updates both the in-memory checkpointLSN and the on-disk
// superblock.WALCheckpointLSN. The metadata write is fsynced before
// the in-memory checkpoint advances, because the caller may recycle
// the corresponding WAL bytes immediately after this method returns.
func (s *WALStore) persistCheckpoint(highestLSN uint64) error {
	return s.persistCheckpointReleasing(highestLSN, 0, 0, false)
}

// persistCheckpointAndRelease is persistCheckpoint that also releases the
// WAL bytes through the entry at entryPhys (entryLen bytes long) when the
// checkpoint reaches highestLSN. The released tail is persisted in the
// same superblock before the writer may reuse those bytes.
func (s *WALStore) persistCheckpointAndRelease(highestLSN, entryPhys, entryLen uint64) error {
	return s.persistCheckpointReleasing(highestLSN, entryPhys, entryLen, true)
}

func (s *WALStore) persistCheckpointReleasing(highestLSN, entryPhys, entryLen uint64, release bool) error {
	if s.v3 != nil {
		return s.persistCheckpointV3(highestLSN, s.v3.recoverGen.Load())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.stickyIOFault(); err != nil {
		return err // no checkpoint advance after a fault, whenever it latched
	}
	requested := highestLSN
	// Clamp the proposed checkpoint to the recycle floor reported
	// by an external coordinator (e.g., the recovery package's
	// PeerShipCoordinator). When a replica is in an active rebuild
	// session and pin_floor < highestLSN, we hold the checkpoint at
	// pin_floor so WAL entries the replica still depends on are
	// retained. INV-RECYCLE-GATED-BY-MIN-ACTIVE-PIN.
	if s.recycleFloorSrc != nil {
		if floor, anyActive := s.recycleFloorSrc.MinPinAcrossActiveSessions(); anyActive {
			if highestLSN > floor {
				highestLSN = floor
			}
		}
	}
	advance := highestLSN > s.checkpointLSN
	checkpoint := s.checkpointLSN
	if advance {
		checkpoint = highestLSN
	}
	// One snapshot of head and tail; the tail this flush releases is
	// persisted here, before the writer may reuse its bytes.
	head, tail := s.wal.bounds()
	if release && checkpoint >= requested {
		tail = s.wal.tailPastEntry(entryPhys, entryLen)
	}
	if !advance && tail == s.sb.WALTail {
		return nil
	}
	if advance {
		if s.metadata != nil {
			if err := s.metadata.BeforeCheckpointAdvance(highestLSN); err != nil {
				return fmt.Errorf("storage: block metadata before checkpoint %d: %w", highestLSN, err)
			}
		}
		// The chunk layer's dirty bitmap must be durable before the checkpoint
		// (and so the WAL tail) can pass the entries it covers.
		if h := s.chunk.Load(); h != nil {
			if err := h.sink.BeforeCheckpointAdvance(highestLSN); err != nil {
				return fmt.Errorf("storage: chunk bitmap before checkpoint %d: %w", highestLSN, err)
			}
		}
	}

	sbCopy := *s.sb
	sbCopy.WALCheckpointLSN = checkpoint
	sbCopy.WALHead = head
	sbCopy.WALTail = tail
	buf := newSimpleByteBuf()
	if _, err := sbCopy.writeTo(buf); err != nil {
		return fmt.Errorf("storage: encode superblock: %w", err)
	}
	var writeErr error
	err := s.syncFile(func() error {
		checkpointWriteStart := time.Now()
		writeErr = s.writeSuperblockMetadata(buf.bytes())
		if s.flusher != nil {
			s.flusher.instr.recordCheckpointWrite(len(buf.bytes()), time.Since(checkpointWriteStart), writeErr)
		}
		return writeErr
	}, func() error {
		checkpointSyncStart := time.Now()
		err := s.syncSuperblockMetadata()
		if s.flusher != nil {
			s.flusher.instr.recordCheckpointSync(time.Since(checkpointSyncStart), err)
		}
		return err
	})
	if writeErr != nil {
		return fmt.Errorf("storage: pwrite superblock: %w", writeErr)
	}
	if err != nil {
		return fmt.Errorf("storage: fsync checkpoint superblock: %w", err)
	}
	s.checkpointLSN = checkpoint
	s.sb.WALCheckpointLSN = checkpoint
	s.sb.WALHead = sbCopy.WALHead
	s.sb.WALTail = sbCopy.WALTail
	s.wal.tailPersisted(sbCopy.WALTail)
	return nil
}

// CheckpointLSN returns the highest LSN whose data has been durably
// written into the extent. Diagnostic only.
func (s *WALStore) CheckpointLSN() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.checkpointLSN
}

// FlushCount returns the total flush cycles the background flusher
// has performed. Diagnostic only.
func (s *WALStore) FlushCount() uint64 {
	if s.flusher == nil {
		return 0
	}
	return s.flusher.FlushCount()
}

// FlusherInstrumentation returns a cumulative checkpoint-pipeline diagnostic
// snapshot.
func (s *WALStore) FlusherInstrumentation() FlusherInstrumentationStatus {
	if s.flusher == nil {
		return FlusherInstrumentationStatus{}
	}
	return s.flusher.instr.snapshot()
}

// WarningWALHoleSuspected is the typed recovery warning for a WAL hole
// followed by valid records (issue #51): reported on /status, it does not
// change readiness.
const WarningWALHoleSuspected = "wal_hole_suspected"

// RecoveryWarner is a store that reports warnings from its last Recover.
type RecoveryWarner interface {
	RecoveryWarnings() []string
}

// RecoveryWarnings returns the warnings of the last Recover (nil when it
// found nothing to warn about).
func (s *WALStore) RecoveryWarnings() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.recoveryWarnings...)
}

// HolesSuspected counts the recoveries of this store that found a WAL hole
// followed by valid records.
func (s *WALStore) HolesSuspected() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.holesSuspected
}

// Recover replays WAL entries past the last checkpoint into the dirty
// map. After Recover, the in-memory state matches what an aborted
// process would have left durably on disk.
//
// Idempotent: calling Recover twice on the same on-disk state yields
// identical results.
func (s *WALStore) Recover() (frontier uint64, err error) {
	// Drain an attached COW window before taking any lower lock. The lease
	// blocks a replacement window and retries a racing first attachment.
	// Legacy fixtures retain their shared barrier ordering with full copying.
	// Order after drain: captureMu -> lifecycleMu -> syncMu -> mu.
	release := s.holdChunkDestructive(false)
	defer release() // also releases captureMu, after all lower locks
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	if s.v3 != nil {
		// A v3 recovery holds the leader role for its whole run, so no W
		// publication or acknowledgement straddles it (walstore_v3.go
		// recoverGen). Order: lifecycleMu -> syncMu -> s.mu.
		s.v3.syncMu.Lock()
		defer s.v3.syncMu.Unlock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.v3 != nil {
		// The store lock orders both epoch boundaries with real map publishers.
		// START invalidates old candidates; END also invalidates candidates
		// prepared while this Recover was in flight, including on failure.
		defer func() {
			if endErr := s.v3.pub.invalidateMapProofV3(); endErr != nil {
				frontier, err = 0, errors.Join(err, endErr)
			}
		}()
		if err := s.v3.pub.invalidateMapProofV3(); err != nil {
			return 0, err
		}
	}
	if s.closed {
		return 0, errors.New("storage: Recover after Close")
	}
	if err := s.stickyIOFault(); err != nil {
		return 0, err
	}

	// Recovery can republish a prefix left by a failed append (including on
	// this same open file). Invalidate before releasing publication, even on
	// a failed scan; the next successful sync establishes a reusable new cut.
	defer s.walMutation.Add(1)
	// The same republication may change any chunk's bytes: an attached
	// chunk layer counts it as a mutation of every chunk (staged pre-copies
	// are never reused across it), also on a failed or partial scan. Runs
	// before s.mu is released, so no read sees recovered bytes first.
	defer s.noteChunkRecovery()
	if s.v3 != nil {
		return s.recoverV3()
	}
	s.dm.clear()
	res, err := recoverWAL(s.fd, s.sb, s.dm)
	if err != nil {
		s.dm.clear()
		return 0, err
	}
	if err := s.wal.restoreRecoveredBounds(res.WALHead, res.WALTail); err != nil {
		s.dm.clear()
		return 0, NewWALIntegrityFailure(err, "restore walstore recovery bounds")
	}
	if res.WALHead != s.sb.WALHead || res.WALTail != s.sb.WALTail {
		// Persist the rebuilt bounds before any append: the writer never
		// reuses bytes the on-disk superblock still declares retained.
		sbCopy := *s.sb
		sbCopy.WALHead, sbCopy.WALTail = res.WALHead, res.WALTail
		buf := newSimpleByteBuf()
		if _, err := sbCopy.writeTo(buf); err != nil {
			s.dm.clear()
			return 0, fmt.Errorf("storage: encode recovered superblock: %w", err)
		}
		var writeErr error
		err := s.syncFile(func() error {
			writeErr = s.writeSuperblockMetadata(buf.bytes())
			return writeErr
		}, func() error { return s.syncSuperblockMetadata() })
		if writeErr != nil {
			s.dm.clear()
			return 0, fmt.Errorf("storage: pwrite recovered superblock: %w", writeErr)
		}
		if err != nil {
			s.dm.clear()
			return 0, fmt.Errorf("storage: fsync recovered superblock: %w", err)
		}
	}
	s.wal.tailPersisted(res.WALTail)
	s.sb.WALHead = res.WALHead
	s.sb.WALTail = res.WALTail
	s.recoveryWarnings = nil
	if res.HoleSuspected {
		s.holesSuspected++
		s.recoveryWarnings = []string{WarningWALHoleSuspected + ": " + res.HoleDetail}
	}
	recoveredHead := s.checkpointLSN
	if res.HighestLSN > recoveredHead {
		recoveredHead = res.HighestLSN
	}
	if recoveredHead >= s.nextLSN {
		s.nextLSN = recoveredHead + 1
	}
	if recoveredHead > s.syncedLSN {
		s.syncedLSN = recoveredHead
	}
	s.walHead = recoveredHead
	if s.walTail == 0 && s.syncedLSN > 0 {
		// First write LSN observed becomes S baseline.
		s.walTail = 1
	}
	return s.syncedLSN, nil
}

// Write appends one block to the WAL with a fresh LSN, updates the
// dirty map, and returns the assigned LSN. The write is NOT durable
// until Sync returns (or until a concurrent group-commit fsync
// covers this LSN). Reads after this Write see the new bytes
// immediately.
func (s *WALStore) Write(lba uint32, data []byte) (uint64, error) {
	return s.WriteContext(context.Background(), lba, data)
}

func (s *WALStore) WriteContext(ctx context.Context, lba uint32, data []byte) (uint64, error) {
	deadline := s.writeAdmissionDeadline(ctx)
	if err := s.acquireWriteLock(ctx, deadline, s.captureMu.TryRLock, s.captureMu.RLock); err != nil {
		return 0, err
	}
	defer s.captureMu.RUnlock()
	if err := s.stickyIOFault(); err != nil {
		return 0, err
	}
	if uint64(lba) >= uint64(s.sb.VolumeSize/uint64(s.sb.BlockSize)) {
		return 0, fmt.Errorf("storage: LBA %d out of range", lba)
	}
	if len(data) != int(s.sb.BlockSize) {
		return 0, fmt.Errorf("storage: data size %d != block size %d", len(data), s.sb.BlockSize)
	}

	// WAL admission: throttle/block under WAL pressure. If admission
	// is disabled (admission==nil), Write proceeds immediately. The
	// admission Acquire may block up to admissionTimeout — under
	// hard pressure it spins waiting for the flusher to drain.
	s.beforeChunkMutation(uint64(lba), 1)
	if err := s.acquireWriteLock(ctx, deadline, s.lifecycleMu.TryRLock, s.lifecycleMu.RLock); err != nil {
		return 0, err
	}
	defer s.lifecycleMu.RUnlock()

	if err := s.writeBudgetError(ctx, deadline); err != nil {
		return 0, err
	}
	if s.admission != nil {
		wait := s.admissionTimeout
		if !deadline.IsZero() {
			wait = time.Until(deadline)
		}
		if err := s.admission.acquireContext(ctx, wait, s.writePressureSample(ctx, deadline)); err != nil {
			return 0, fmt.Errorf("storage: WAL admission: %w", err)
		}
		defer s.admission.Release()
	}
	if h := s.testAfterAdmit; h != nil {
		h()
	}

	commitLockStart := time.Now()
	s.mu.Lock() // a short lock: blocking keeps RWMutex writer preference (#252); the budget is checked below
	s.instr.recordWriteCommitLockWait(time.Since(commitLockStart))
	if s.closed {
		s.mu.Unlock()
		return 0, errors.New("storage: Write after Close")
	}
	// Recheck under the append lock: an append that failed while this call
	// waited latched the fault and left nextLSN and the head unmoved.
	if err := s.stickyIOFault(); err != nil {
		s.mu.Unlock()
		return 0, err
	}
	if err := s.writeBudgetError(ctx, deadline); err != nil {
		s.mu.Unlock()
		return 0, err
	}
	lsn := s.nextLSN

	entry := &walEntry{
		LSN:    lsn,
		Type:   walEntryWrite,
		LBA:    uint64(lba),
		Length: uint32(len(data)),
		Data:   data,
	}
	appendStart := time.Now()
	var walRelOff uint64
	var err error
	if s.v3 != nil {
		walRelOff, err = s.appendWriteV3(lba, data, lsn, func() error { return s.writeBudgetError(ctx, deadline) })
	} else {
		walRelOff, err = s.wal.append(entry)
	}
	s.instr.appendLatency.Observe(time.Since(appendStart))
	// A failed pwrite may still leave complete records or padding in the file.
	s.walMutation.Add(1)
	if err != nil {
		if s.v3 == nil {
			s.noteAppendFailure(err)
		}
		s.mu.Unlock()
		return 0, fmt.Errorf("storage: WAL append: %w", err)
	}
	dirtyStart := time.Now()
	s.dm.put(uint64(lba), walRelOff, lsn, uint32(len(data)))
	s.noteChunkMutation(uint64(lba))
	s.instr.recordDirtyMapUpdate(1, time.Since(dirtyStart))

	s.nextLSN++
	if lsn > s.walHead {
		s.walHead = lsn
	}
	if s.walTail == 0 {
		s.walTail = lsn
	}
	s.mu.Unlock()
	return lsn, nil
}

func (s *WALStore) WriteBatch(startLBA uint32, blocks [][]byte) ([]uint64, error) {
	return s.WriteBatchContext(context.Background(), startLBA, blocks)
}

func (s *WALStore) WriteBatchContext(ctx context.Context, startLBA uint32, blocks [][]byte) ([]uint64, error) {
	deadline := s.writeAdmissionDeadline(ctx)
	if err := s.acquireWriteLock(ctx, deadline, s.captureMu.TryRLock, s.captureMu.RLock); err != nil {
		return nil, err
	}
	defer s.captureMu.RUnlock()
	if err := s.stickyIOFault(); err != nil {
		return nil, err
	}
	if len(blocks) == 0 {
		return nil, nil
	}
	maxLBA := uint32(s.sb.VolumeSize / uint64(s.sb.BlockSize))
	if uint64(startLBA)+uint64(len(blocks)) > uint64(maxLBA) {
		return nil, fmt.Errorf("storage: batch [%d,%d) out of range (max %d)", startLBA, uint64(startLBA)+uint64(len(blocks)), maxLBA)
	}
	for i, data := range blocks {
		if len(data) != int(s.sb.BlockSize) {
			return nil, fmt.Errorf("storage: batch block %d data size %d != block size %d", i, len(data), s.sb.BlockSize)
		}
	}

	s.beforeChunkMutation(uint64(startLBA), uint64(len(blocks)))
	if err := s.acquireWriteLock(ctx, deadline, s.lifecycleMu.TryRLock, s.lifecycleMu.RLock); err != nil {
		return nil, err
	}
	defer s.lifecycleMu.RUnlock()

	if err := s.writeBudgetError(ctx, deadline); err != nil {
		return nil, err
	}
	if s.admission != nil {
		wait := s.admissionTimeout
		if !deadline.IsZero() {
			wait = time.Until(deadline)
		}
		if err := s.admission.acquireContext(ctx, wait, s.writePressureSample(ctx, deadline)); err != nil {
			return nil, fmt.Errorf("storage: WAL admission: %w", err)
		}
		defer s.admission.Release()
	}
	if h := s.testAfterAdmit; h != nil {
		h()
	}

	commitLockStart := time.Now()
	s.mu.Lock() // a short lock: blocking keeps RWMutex writer preference (#252); the budget is checked below
	s.instr.recordWriteCommitLockWait(time.Since(commitLockStart))
	if s.closed {
		s.mu.Unlock()
		return nil, errors.New("storage: WriteBatch after Close")
	}
	if err := s.stickyIOFault(); err != nil { // recheck under the append lock
		s.mu.Unlock()
		return nil, err
	}
	if err := s.writeBudgetError(ctx, deadline); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	firstLSN := s.nextLSN
	if s.v3 != nil {
		lsns, err := s.writeBatchV3Locked(startLBA, blocks, firstLSN, func() error { return s.writeBudgetError(ctx, deadline) })
		s.mu.Unlock()
		return lsns, err
	}
	useMultiBlock := s.multiBlockRecords && len(blocks) > 1

	if useMultiBlock {
		lsns, err := s.writeBatchMultiBlockLocked(startLBA, blocks, firstLSN)
		s.mu.Unlock()
		return lsns, err
	}

	entries := make([]walEntry, len(blocks))
	lsns := make([]uint64, len(blocks))
	for i, data := range blocks {
		lsn := firstLSN + uint64(i)
		lsns[i] = lsn
		entries[i] = walEntry{
			LSN:    lsn,
			Type:   walEntryWrite,
			LBA:    uint64(startLBA + uint32(i)),
			Length: uint32(len(data)),
			Data:   data,
		}
	}
	appendStart := time.Now()
	offsets, err := s.wal.appendBatch(entries)
	s.instr.appendLatency.Observe(time.Since(appendStart))
	// A failed pwrite may still leave complete records or padding in the file.
	s.walMutation.Add(1)
	if err != nil {
		s.noteAppendFailure(err)
		s.mu.Unlock()
		return nil, fmt.Errorf("storage: WAL batch append: %w", err)
	}
	dirtyStart := time.Now()
	for i, walRelOff := range offsets {
		s.dm.put(uint64(startLBA+uint32(i)), walRelOff, lsns[i], uint32(len(blocks[i])))
		s.noteChunkMutation(uint64(startLBA + uint32(i)))
	}
	s.instr.recordDirtyMapUpdate(len(offsets), time.Since(dirtyStart))

	lastLSN := lsns[len(lsns)-1]
	s.nextLSN += uint64(len(lsns))
	if lastLSN > s.walHead {
		s.walHead = lastLSN
	}
	if s.walTail == 0 {
		s.walTail = firstLSN
	}
	s.mu.Unlock()
	return lsns, nil
}

// writeBatchMultiBlockLocked appends one multi-block record while the caller
// holds s.mu, so a failed append cannot consume an LSN range.
func (s *WALStore) writeBatchMultiBlockLocked(startLBA uint32, blocks [][]byte, firstLSN uint64) ([]uint64, error) {
	totalLen := 0
	for _, data := range blocks {
		totalLen += len(data)
	}
	payload := make([]byte, 0, totalLen)
	for _, data := range blocks {
		payload = append(payload, data...)
	}
	entry := &walEntry{
		LSN:      firstLSN,
		Reserved: uint64(len(blocks)),
		Type:     walEntryWriteBatch,
		LBA:      uint64(startLBA),
		Length:   uint32(len(payload)),
		Data:     payload,
	}
	appendStart := time.Now()
	walRelOff, err := s.wal.append(entry)
	s.instr.appendLatency.Observe(time.Since(appendStart))
	// A failed pwrite may still leave complete records or padding in the file.
	s.walMutation.Add(1)
	if err != nil {
		s.noteAppendFailure(err)
		return nil, fmt.Errorf("storage: WAL multiblock append: %w", err)
	}

	lsns := make([]uint64, len(blocks))
	dirtyStart := time.Now()
	for i, data := range blocks {
		lsn := firstLSN + uint64(i)
		lsns[i] = lsn
		s.dm.putAt(
			uint64(startLBA+uint32(i)),
			walRelOff,
			uint32(i*int(s.sb.BlockSize)),
			lsn,
			uint32(len(data)),
		)
		s.noteChunkMutation(uint64(startLBA + uint32(i)))
	}
	s.instr.recordDirtyMapUpdate(len(blocks), time.Since(dirtyStart))

	lastLSN := lsns[len(lsns)-1]
	s.nextLSN += uint64(len(lsns))
	if lastLSN > s.walHead {
		s.walHead = lastLSN
	}
	if s.walTail == 0 {
		s.walTail = firstLSN
	}
	return lsns, nil
}

func (s *WALStore) WriteInstrumentation() WriteInstrumentationStatus {
	return s.instr.snapshot()
}

// Read returns the current bytes at lba. Dirty entries are served
// from the WAL; clean LBAs are served from the extent.
func (s *WALStore) Read(lba uint32) ([]byte, error) { return s.ReadContext(context.Background(), lba) }

func (s *WALStore) ReadContext(ctx context.Context, lba uint32) (data []byte, err error) {
	defer func() {
		if err == nil && ctx.Err() != nil {
			data = nil
			err = hydrationPending(ctx.Err())
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, hydrationPending(err)
	}
	maxLBA := uint32(s.sb.VolumeSize / uint64(s.sb.BlockSize))
	if lba >= maxLBA {
		return nil, fmt.Errorf("storage: LBA %d out of range", lba)
	}
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, errors.New("storage: Read after Close")
	}
	s.mu.RUnlock()

	if s.v3 != nil {
		if data, handled, err := s.readDirtyV3(lba); handled {
			return data, err
		}
		return s.readFromExtentContext(ctx, lba)
	}
	if walRelOff, dataOffset, _, _, ok := s.dm.get(uint64(lba)); ok {
		return s.readFromWAL(walRelOff, dataOffset)
	}
	return s.readFromExtentContext(ctx, lba)
}

// readFromWAL pulls a single block out of a WAL Write/Trim entry
// previously deposited by Append. The walRelOff points at the start
// of the entry; data starts at walEntryHeaderSize bytes into it.
func (s *WALStore) readFromWAL(walRelOff uint64, dataOffset uint32) ([]byte, error) {
	headerBuf := make([]byte, walEntryHeaderSize)
	absOff := int64(s.sb.WALOffset + walRelOff)
	if _, err := s.fd.ReadAt(headerBuf, absOff); err != nil {
		return nil, fmt.Errorf("storage: WAL read header: %w", err)
	}
	if headerBuf[16] == walEntryTrim {
		return make([]byte, s.sb.BlockSize), nil
	}
	length := parseLengthFromHeader(headerBuf)
	if dataOffset >= length {
		return nil, fmt.Errorf("storage: WAL read data offset %d >= length %d", dataOffset, length)
	}
	readLen := s.sb.BlockSize
	if remaining := length - dataOffset; remaining < readLen {
		readLen = remaining
	}
	data := make([]byte, readLen)
	if _, err := s.fd.ReadAt(data, absOff+int64(walEntryPrefixSize)+int64(dataOffset)); err != nil {
		return nil, fmt.Errorf("storage: WAL read data: %w", err)
	}
	// In the simple "one block per WAL write" case, length == blockSize.
	if uint32(len(data)) > s.sb.BlockSize {
		data = data[:s.sb.BlockSize]
	}
	return data, nil
}

// SetMultiBlockRecords toggles the Phase 150 disabled-by-default multi-block
// WAL record prototype. Callers must keep this behind an explicit operator/test
// opt-in until mounted NVMe/TCP profiling and format-compatibility gates pass.
func (s *WALStore) SetMultiBlockRecords(enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.multiBlockRecords = enabled
}

func (s *WALStore) enableMultiBlockRecordsForTest(enabled bool) {
	s.SetMultiBlockRecords(enabled)
}

// DisableAutoFlushForRecoveryTest stops the background WAL->extent flusher
// before test writes are issued. This preserves synced WAL records past a
// process restart so mounted recovery gates can prove actual WAL replay instead
// of reading data that was already checkpointed into the extent.
//
// This is intentionally not part of LogicalStorage and must stay behind an
// explicit test/diagnostic flag. Calling it after writes may flush the current
// dirty set because flusher.Stop performs one final best-effort flush.
func (s *WALStore) DisableAutoFlushForRecoveryTest() {
	if s.flusher != nil {
		s.flusher.Stop()
	}
}

func (s *WALStore) readFromExtent(lba uint32) ([]byte, error) {
	return s.readFromExtentContext(context.Background(), lba)
}

func (s *WALStore) readFromExtentContext(ctx context.Context, lba uint32) ([]byte, error) {
	// An attached chunk layer resolves blocks that are absent locally
	// before the extent lock is taken; nil data means read the extent.
	if h := s.chunk.Load(); h != nil {
		var data []byte
		var err error
		if r, ok := h.sink.(interface {
			OnReadMissContext(context.Context, uint64) ([]byte, error)
		}); ok {
			data, err = r.OnReadMissContext(ctx, uint64(lba))
		} else {
			data, err = h.sink.OnReadMiss(uint64(lba))
		}
		if err != nil {
			return nil, err
		}
		if data != nil {
			return data, nil
		}
		// The miss may have waited on a remote fetch while this block was
		// written: re-resolve through the normal path (dirty map/WAL first)
		// before the extent. No lock is held here.
		if s.v3 != nil {
			if data, handled, err := s.readDirtyV3(lba); handled {
				return data, err
			}
		} else if walRelOff, dataOffset, _, _, ok := s.dm.get(uint64(lba)); ok {
			return s.readFromWAL(walRelOff, dataOffset)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, hydrationPending(err)
	}
	return s.readExtentBlock(lba)
}

// readExtentBlock reads lba's extent bytes under its extent lock, without the
// chunk layer's miss hook: the caller has resolved the block as local.
func (s *WALStore) readExtentBlock(lba uint32) ([]byte, error) {
	lock := &s.extentMu[uint64(lba)%uint64(len(s.extentMu))]
	lock.RLock()
	defer lock.RUnlock()
	data := make([]byte, s.sb.BlockSize)
	off := int64(s.extentBase + uint64(lba)*uint64(s.sb.BlockSize))
	if _, err := s.fd.ReadAt(data, off); err != nil {
		return nil, fmt.Errorf("storage: extent read LBA %d: %w", lba, err)
	}
	if s.metadata != nil {
		if err := s.metadata.VerifyExtent(lba, data); err != nil {
			return nil, err
		}
	}
	return data, nil
}

// Sync forces all in-flight writes to durable storage and returns the
// stable frontier (the highest LSN that is now durable).
//
// Sync is the durability boundary. A write returned-from-Write
// becomes crash-survivable only when a Sync that covers its LSN
// returns nil. Group commit batches concurrent Sync callers into one
// fsync.
func (s *WALStore) Sync() (uint64, error) {
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return 0, errors.New("storage: Sync after Close")
	}
	if err := s.stickyIOFault(); err != nil {
		s.mu.RUnlock()
		return 0, err
	}
	targetHead := s.walHead
	targetDirectFrontier := s.pendingDirectFrontierLSN
	var targetGen uint64
	if s.v3 != nil {
		// The logical Sync is gated while installing (DESIGN 7): exactly
		// (0, ErrBaseInstalling), no batch, no new W.
		if s.v3.pub.current().BaseState == BaseInstalling {
			s.mu.RUnlock()
			return 0, ErrBaseInstalling
		}
		targetGen = s.v3.recoverGen.Load() // captured with targetHead (Recover holds s.mu)
	}
	s.mu.RUnlock()

	if s.walSyncCovered() {
		// Preserve shutdown refusal even when no file I/O is necessary.
		select {
		case <-s.committer.stopCh:
			return 0, ErrGroupCommitShutdown
		default:
		}
	} else if err := s.syncCache(); err != nil {
		return 0, fmt.Errorf("storage: group commit fsync: %w", err)
	}
	s.syncs.Add(1)

	if v := s.v3; v != nil {
		if h := v.testSyncBeforeCompletion; h != nil {
			h()
		}
		// The completion is validated against the recovery generation under
		// the leader role: a recovery since the capture refuses the ack.
		v.syncMu.Lock()
		defer v.syncMu.Unlock()
		if v.recoverGen.Load() != targetGen {
			return 0, errV3RecoveredMeanwhile
		}
		if v.pub.current().BaseState == BaseInstalling {
			return 0, ErrBaseInstalling // an install began meanwhile
		}
	}
	s.mu.Lock()
	// A fault latched while this Sync waited (another caller's fsync or
	// append, or a flusher fsync) refuses it: nothing is acknowledged.
	if err := s.stickyIOFault(); err != nil {
		s.mu.Unlock()
		return 0, err
	}
	if targetDirectFrontier > s.checkpointLSN && targetDirectFrontier <= targetHead {
		if s.metadata != nil {
			if err := s.metadata.BeforeCheckpointAdvance(targetDirectFrontier); err != nil {
				s.mu.Unlock()
				return 0, fmt.Errorf("storage: block metadata before direct frontier %d: %w", targetDirectFrontier, err)
			}
		}
		if h := s.chunk.Load(); h != nil {
			if err := h.sink.BeforeCheckpointAdvance(targetDirectFrontier); err != nil {
				s.mu.Unlock()
				return 0, fmt.Errorf("storage: chunk bitmap before direct frontier %d: %w", targetDirectFrontier, err)
			}
		}
		sbCopy := *s.sb
		sbCopy.WALCheckpointLSN = targetDirectFrontier
		// WALHead is a logical byte cursor, not an LSN. Holding s.mu
		// excludes appends while the second fsync makes this cursor durable.
		sbCopy.WALHead = s.wal.logicalHeadValue()
		buf := newSimpleByteBuf()
		if _, err := sbCopy.writeTo(buf); err != nil {
			s.mu.Unlock()
			return 0, fmt.Errorf("storage: encode superblock: %w", err)
		}
		if err := s.syncFile(nil, func() error { return s.syncDirectFrontierMetadata(buf.bytes()) }); err != nil {
			s.mu.Unlock()
			return 0, err
		}
		s.checkpointLSN = targetDirectFrontier
		s.sb.WALCheckpointLSN = targetDirectFrontier
		s.sb.WALHead = sbCopy.WALHead
		if s.pendingDirectFrontierLSN <= targetDirectFrontier {
			s.pendingDirectFrontierLSN = 0
		}
	}
	if targetHead > s.syncedLSN {
		s.syncedLSN = targetHead
	}
	frontier := s.syncedLSN
	s.mu.Unlock()
	return frontier, nil
}

// Boundaries returns the current R/S/H boundaries — what is durable,
// what the WAL still retains, what's the newest write.
func (s *WALStore) Boundaries() (R, S, H uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.syncedLSN, s.walTail, s.walHead
}

// NextLSN returns the LSN that the next Write will receive.
func (s *WALStore) NextLSN() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextLSN
}

// NumBlocks returns the addressable block count.
func (s *WALStore) NumBlocks() uint32 {
	return uint32(s.sb.VolumeSize / uint64(s.sb.BlockSize))
}

// BlockSize returns the IO unit size in bytes.
func (s *WALStore) BlockSize() int { return int(s.sb.BlockSize) }

// SetRecoveryRetentionLSNs configures the WAL retention window past
// checkpointLSN. After this is set, the recovery scan accepts
// fromLSN > checkpointLSN - retentionLSNs. Zero (the default)
// preserves strict checkpoint-driven recycle.
//
// Operator-tunable via blockvolume's --wal-retention-lsns flag.
// In-memory only; not persisted (re-applied on restart from CLI).
//
// Called by: DurableProvider construction path after walstore is
// opened, with the operator-supplied value from cmd/blockvolume.
// Owns: recoveryRetentionLSNs field under s.mu.
// SetRecycleFloorSource installs the gate consulted in
// persistCheckpoint to clamp checkpoint advancement against active
// recover-session pin floors. Pass nil to disable.
//
// Idempotent. Safe to call at any time — the gate is read under
// s.mu so concurrent persistCheckpoint sees a consistent value.
//
// Implements storage.RecycleFloorGate.
func (s *WALStore) SetRecycleFloorSource(src RecycleFloorSource) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recycleFloorSrc = src
}

func (s *WALStore) SetRecoveryRetentionLSNs(n uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recoveryRetentionLSNs = n
}

// RecoveryRetentionLSNs returns the currently-configured retention
// window past checkpointLSN. Test/diagnostic accessor.
func (s *WALStore) RecoveryRetentionLSNs() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.recoveryRetentionLSNs
}

// AdvanceFrontier bumps the recorded frontier without writing data.
// Used by the rebuild server to declare the replica's frontier
// matches the primary's head once base blocks are installed.
func (s *WALStore) AdvanceFrontier(lsn uint64) {
	if s.v3 != nil {
		return // v3 frontier jumps happen only inside MarkBaseComplete (rows 9 to 11)
	}
	s.bypassSyncDedup.Store(true)
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if lsn >= s.nextLSN {
		s.nextLSN = lsn + 1
	}
	if lsn > s.walHead {
		s.walHead = lsn
	}
	if lsn > s.pendingDirectFrontierLSN {
		s.pendingDirectFrontierLSN = lsn
	}
}

// AdvanceWALTail moves the retained-window tail forward. After this,
// recovery cases where the requested LSN is below newTail must
// escalate to rebuild.
func (s *WALStore) AdvanceWALTail(newTail uint64) {
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if newTail > s.walTail {
		s.walTail = newTail
	}
}

// WriteExtentDirect installs a base block directly into the extent
// without going through the WAL append path — INV-RECV-BITMAP-CORE
// (§6.10). The receiver's per-session bitmap is the sole arbiter of
// BASE-vs-WAL conflict at this LBA; substrate-level WAL replay /
// stale-skip is intentionally bypassed.
//
// No LSN is recorded. nextLSN / walHead / dirtyMap are NOT advanced.
// The recovery layer pairs this with AdvanceFrontier(targetLSN) at
// MarkBaseComplete to keep post-rebuild frontier reporting honest.
//
// Durability follows the same rule as Write: bytes become durable
// only on the next successful Sync (the extent fsync covers them).
func (s *WALStore) WriteExtentDirect(lba uint32, data []byte) error {
	if s.v3 != nil {
		return errV3NotYet // v3 BASE writes go through an install handle (rows 9 to 11)
	}
	s.bypassSyncDedup.Store(true)
	s.captureMu.RLock()
	defer s.captureMu.RUnlock()
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	if err := s.stickyIOFault(); err != nil {
		return err
	}
	maxLBA := uint32(s.sb.VolumeSize / uint64(s.sb.BlockSize))
	if lba >= maxLBA {
		return fmt.Errorf("storage: WriteExtentDirect LBA %d out of range", lba)
	}
	if len(data) != int(s.sb.BlockSize) {
		return fmt.Errorf("storage: WriteExtentDirect data size %d != block size %d", len(data), s.sb.BlockSize)
	}
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return errors.New("storage: WriteExtentDirect after Close")
	}
	lock := &s.extentMu[uint64(lba)%uint64(len(s.extentMu))]
	lock.Lock()
	defer lock.Unlock()
	_, _, previousLSN, _, hadDirty := s.dm.get(uint64(lba))
	if err := s.writeExtentUnlocked(lba, data); err != nil {
		return err
	}
	if h := s.chunk.Load(); h != nil {
		h.sink.OnExtentWrite(uint64(lba), 0)
	}
	if hadDirty {
		s.dm.compareAndDelete(uint64(lba), previousLSN)
	}
	return nil
}

// ApplyEntry writes a replicated block with the source's LSN rather
// than allocating a fresh one. Same durability semantics as Write
// (becomes durable on next Sync).
// noteChunkRecovery tells an attached chunk layer that Recover republished
// the dirty map: any chunk may have changed.
func (s *WALStore) noteChunkRecovery() {
	if h := s.chunk.Load(); h != nil && h.recovered != nil {
		h.recovered()
	}
}

// noteChunkMutation tells an attached chunk layer that lba changed; it
// runs after the dirty-map insert that made the change visible.
func (s *WALStore) noteChunkMutation(lba uint64) {
	if h := s.chunk.Load(); h != nil && h.mutated != nil {
		h.mutated(lba)
	}
}

func (s *WALStore) ApplyEntry(lba uint32, data []byte, lsn uint64) error {
	if s.v3 != nil {
		return s.applyEntryV3(lba, data, lsn, false)
	}
	s.bypassSyncDedup.Store(true)
	s.captureMu.RLock()
	defer s.captureMu.RUnlock()
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	if err := s.stickyIOFault(); err != nil {
		return err
	}
	maxLBA := uint32(s.sb.VolumeSize / uint64(s.sb.BlockSize))
	if lba >= maxLBA {
		return fmt.Errorf("storage: apply LBA %d out of range", lba)
	}
	if len(data) != int(s.sb.BlockSize) {
		return fmt.Errorf("storage: apply data size %d != block size %d", len(data), s.sb.BlockSize)
	}
	if h := s.testAfterAdmit; h != nil {
		h()
	}
	// Publish under the same lock as Write/WriteBatch and the flusher's
	// cross-shard snapshot, including supplied LSNs below nextLSN.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("storage: ApplyEntry after Close")
	}
	if err := s.stickyIOFault(); err != nil { // recheck under the append lock
		return err
	}
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	entry := &walEntry{
		LSN:    lsn,
		Type:   walEntryWrite,
		LBA:    uint64(lba),
		Length: uint32(len(dataCopy)),
		Data:   dataCopy,
	}
	appendStart := time.Now()
	walRelOff, err := s.wal.append(entry)
	s.instr.appendLatency.Observe(time.Since(appendStart))
	// A failed pwrite may still leave complete records or padding in the file.
	s.walMutation.Add(1)
	if err != nil {
		s.noteAppendFailure(err)
		return fmt.Errorf("storage: WAL append (apply): %w", err)
	}
	s.dm.put(uint64(lba), walRelOff, lsn, uint32(len(dataCopy)))
	s.noteChunkMutation(uint64(lba))

	if lsn >= s.nextLSN {
		s.nextLSN = lsn + 1
	}
	if lsn > s.walHead {
		s.walHead = lsn
	}
	return nil
}

// RecoveryMode reports walstore's recovery sub-mode. walstore's
// ScanLBAs emits per-LSN entries from the retained WAL.
func (s *WALStore) RecoveryMode() RecoveryMode {
	return RecoveryModeWALReplay
}

// AppliedLSNs returns a partial view of per-LBA applied LSN: only
// LBAs whose latest write is still in the WAL (not yet flushed to
// extent) are reported. Once an entry is flushed and the dirty-map
// entry is cleared, walstore loses per-LBA LSN tracking — the
// extent stores data only, not per-LBA LSN.
//
// PARTIAL-VIEW LIMITATION: for full per-LBA applied-LSN tracking,
// walstore would need a permanent per-LBA LSN map (substrate
// refactor). The replica recovery apply gate is the authoritative
// correctness boundary; this partial seed is defense-in-depth —
// it correctly stale-skips recovery entries for LBAs still in the
// WAL window, and falls back to "appliedLSN[LBA] = 0" semantics
// for flushed LBAs (which means recovery WILL apply them —
// acceptable when the gate's session-only tracking + live-lane
// updates fill in the gap during the session).
//
// Called by: replica recovery apply gate at session start.
// Owns: per-call snapshot of dirty map (lock-free under shard locks).
// Borrows: nothing (returned map is fresh and caller-owned).
func (s *WALStore) AppliedLSNs() (map[uint32]uint64, error) {
	entries := s.dm.snapshot()
	out := make(map[uint32]uint64, len(entries))
	for _, e := range entries {
		lba := uint32(e.LBA)
		if existing, ok := out[lba]; !ok || e.LSN > existing {
			out[lba] = e.LSN
		}
	}
	return out, nil
}

// AllBlocks snapshots every written LBA's current bytes. Reads
// every LBA in the volume via Read() (which checks the dirty map
// first, falls back to the extent) and returns the entries whose
// content is non-zero.
//
// This honors the LogicalStorage contract: "snapshots every
// written LBA's current bytes" — including LBAs whose data has
// been flushed out of the dirty map into the extent. Filtering
// zeros preserves the "snapshot of dirty bits" semantics callers
// expect (a never-written LBA shouldn't appear in the output;
// neither should an LBA that was trimmed back to zeros).
//
// Linear scan over numBlocks. Acceptable for the rebuild path
// (one-shot, off the hot read/write loop). Not appropriate for
// a hot-path scan.
func (s *WALStore) AllBlocks() (map[uint32][]byte, error) {
	n := s.NumBlocks()
	out := make(map[uint32][]byte)
	zero := make([]byte, s.sb.BlockSize)
	for lba := uint32(0); lba < n; lba++ {
		data, err := s.Read(lba)
		if err != nil {
			return nil, fmt.Errorf("snapshot LBA %d: %w", lba, err)
		}
		// Skip never-written / trimmed LBAs.
		if bytesAllZero(data, zero) {
			continue
		}
		out[lba] = data
	}
	return out, nil
}

// bytesAllZero is a fast tight-loop equality against a zero slice.
// We pass zero in to avoid allocating it per LBA.
func bytesAllZero(data, zero []byte) bool {
	if len(data) != len(zero) {
		return false
	}
	for i, b := range data {
		if b != zero[i] {
			return false
		}
	}
	return true
}

// Close persists current WAL boundaries into the superblock,
// fsyncs the file, stops the group committer, and releases the
// underlying file. Idempotent.
func (s *WALStore) Close() (err error) {
	s.captureMu.Lock()
	if s.attachClosed {
		done := s.closeComplete
		s.captureMu.Unlock()
		<-done
		return s.closeResult
	}
	s.attachClosed = true
	s.closeComplete = make(chan struct{})
	h := s.chunk.Load()
	s.captureMu.Unlock()
	defer func() { s.closeResult = err; close(s.closeComplete) }()
	if s.testCloseSnapshot != nil {
		s.testCloseSnapshot()
	}
	// No capture or store lock is held while callbacks drain actual I/O.
	if h != nil {
		_ = h.closer()
	}

	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	// Stop the flusher first so it can't race the file close. In the normal
	// path, flusher.Stop performs one final best-effort flush so any pending
	// dirty entries get into the extent + checkpoint before we release the
	// file. Test gates may have already stopped it via
	// DisableAutoFlushForRecoveryTest; in that case Stop is idempotent and does
	// not checkpoint later writes.
	var flushErr error
	if s.flusher != nil {
		flushErr = s.flusher.Stop()
	}
	if s.committer != nil {
		s.committer.Stop()
	}
	var closeErrors []error
	if flushErr != nil {
		closeErrors = append(closeErrors, fmt.Errorf("storage: final flush: %w", flushErr))
	}
	if fault := s.stickyIOFault(); fault != nil {
		closeErrors = append(closeErrors, fault)
	}
	if s.fd != nil && s.v3 == nil && s.stickyIOFault() == nil {
		// v2 only: a v3 Close writes nothing; its acknowledged state is
		// already durable in the descriptor slots and the WAL, and a
		// lagged mode-C W is recovered from the WAL scan on reopen.
		// Persist current head/tail back into the superblock so a
		// subsequent OpenWALStore can find the WAL bounds without
		// relying solely on the defensive scan.
		s.sb.WALHead, s.sb.WALTail = s.wal.bounds()
		buf := newSimpleByteBuf()
		if _, err := s.sb.writeTo(buf); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("storage: encode final superblock: %w", err))
		} else {
			if err := s.writeSuperblockMetadata(buf.bytes()); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("storage: write final superblock: %w", err))
			}
			if err := s.syncSuperblockMetadata(); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("storage: fsync final superblock: %w", err))
			}
		}
	}
	if s.v3 != nil && s.v3.ownedMap != nil {
		if err := s.v3.ownedMap.Close(); err != nil {
			closeErrors = append(closeErrors, err)
		}
		s.v3.ownedMap = nil
	}
	if s.fd != nil {
		closeErr := s.fd.Close()
		s.fd = nil
		if closeErr != nil {
			closeErrors = append(closeErrors, closeErr)
		}
	}
	return errors.Join(closeErrors...)
}

// Compile-time assertion: WALStore satisfies LogicalStorage.
var _ LogicalStorage = (*WALStore)(nil)

// WALSyncCount returns physical WAL sync attempts (including errors), not logical
// Sync calls or the separate checkpoint/extent metadata sync operations.
func (s *WALStore) WALSyncCount() uint64 { return s.walSyncCalls.Load() }

// walSyncCovered is used only after the caller has captured its publication
// frontier. A later concurrent write may force extra I/O, never a false ACK.
func (s *WALStore) walSyncCovered() bool {
	return !s.bypassSyncDedup.Load() && s.chunk.Load() == nil &&
		s.walMutation.Load() <= s.durableMutation.Load()
}

// syncWALBatch runs only on the existing committer's single leader. It takes no
// lifecycle/publication/WAL/shard locks and the queue mutex is already released.
// Requests queued during a sync can reuse it only if their mutations predate its
// captured cut. Failure leaves the previous durable generation unchanged.
func (s *WALStore) syncWALBatch() error {
	if s.v3 != nil {
		return s.syncWALBatchV3()
	}
	// A batch queued behind a failed fsync must not fsync again (or report
	// a covered cut as durable): the failed fsync may have dropped pages.
	if err := s.stickyIOFault(); err != nil {
		return err
	}
	cut := s.walMutation.Load()
	if !s.bypassSyncDedup.Load() && s.chunk.Load() == nil && cut <= s.durableMutation.Load() {
		return nil
	}
	start := time.Now()
	defer func() { s.fileSyncLatency.Observe(time.Since(start)) }()
	if err := s.syncFile(nil, func() error { return s.syncWALFile() }); err != nil {
		return err
	}
	s.durableMutation.Store(cut)
	return nil
}

// noteFsyncFailure latches the first fsync failure of the store's file as
// the sticky ErrFsyncFailed fault and returns err unchanged (nil stays nil).
func (s *WALStore) noteFsyncFailure(err error) error {
	if err == nil {
		return nil
	}
	s.ioFault.CompareAndSwap(nil, NewSubstrateIOFailure(
		fmt.Errorf("%w: %w", ErrFsyncFailed, err),
		"fsync failed; the open store refuses further Syncs, writes and checkpoints",
	))
	return err
}

// noteAppendFailure latches a WAL append whose write failed as the sticky
// ErrAppendFailed fault and returns err unchanged. The failed records may be
// on disk without nextLSN or the head having moved; any further append on
// this open store would reuse their LSNs and offsets. Space and encoding
// refusals (no bytes written) do not latch.
func (s *WALStore) noteAppendFailure(err error) error {
	if err == nil || !errors.Is(err, errWALPwrite) {
		return err
	}
	s.ioFault.CompareAndSwap(nil, NewSubstrateIOFailure(
		fmt.Errorf("%w: %w", ErrAppendFailed, err),
		"WAL append failed; the open store refuses further appends and Syncs",
	))
	return err
}

// syncFile runs one fsync of the store's file under fileSyncMu, optionally
// preceded by the pwrite it makes durable (a superblock publication). It is
// refused if a fault is latched, and an fsync failure latches the fault
// before the lock is released, so no publication passes the check and then
// completes after another path's fsync failed. A pwrite failure is returned
// as is (it does not latch: nothing was fsynced).
func (s *WALStore) syncFile(write, sync func() error) error {
	if h := s.testBeforeFileSync; h != nil {
		h()
	}
	s.fileSyncMu.Lock()
	defer s.fileSyncMu.Unlock()
	if err := s.stickyIOFault(); err != nil {
		return err
	}
	if write != nil {
		if err := write(); err != nil {
			return err
		}
	}
	return s.noteFsyncFailure(sync())
}

// stickyIOFault reports the latched I/O fault (fsync or append), if any.
func (s *WALStore) stickyIOFault() error {
	if f := s.ioFault.Load(); f != nil {
		return f
	}
	return nil
}
