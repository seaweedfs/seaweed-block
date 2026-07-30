package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
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

	lifecycleMu sync.RWMutex
	mu          sync.RWMutex
	extentMu    [64]sync.RWMutex
	closed      bool
	nextLSN     uint64 // next LSN to assign to a write
	syncedLSN   uint64 // highest LSN durably present in extent OR WAL after fsync
	walTail     uint64 // exposed S boundary (oldest retained LSN)
	walHead     uint64 // exposed H boundary (newest written LSN)

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

	syncs atomic.Uint64 // total fsync operations performed (test/diagnostic)
	instr writeInstrumentation

	// multiBlockRecords is a disabled-by-default Phase 148 local prototype.
	// It must not be wired into production paths before version/compatibility
	// gates and mounted NVMe/TCP profiling pass.
	multiBlockRecords bool

	// singleReadMaterialization is a disabled-by-default Phase 172 comparison
	// path. It changes only how the flusher reads one WAL record; decoded bytes
	// still pass the existing CRC and semantic validation before use.
	singleReadMaterialization atomic.Bool

	// sharedRecordMaterialization permits one decoded WAL record to serve
	// adjacent snapshot entries with the same exact record identity. The
	// flusher owns this bounded, cycle-local cache.
	sharedRecordMaterialization atomic.Bool
}

// CreateWALStore initializes a new store file at path. Fails if path
// already exists. Use OpenWALStore for the recovery path.
//
// numBlocks defines the addressable block range; blockSize is the IO
// unit (default 4096 if zero). The on-disk file is preallocated to
// hold the superblock + WAL region + a fully addressable extent.
func CreateWALStore(path string, numBlocks uint32, blockSize int) (*WALStore, error) {
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
		ImplVersion: WALStoreImplVersion,
	})
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	sb.CreatedAt = uint64(time.Now().UnixNano())

	totalSize := int64(superblockSize) + int64(sb.WALSize) + int64(sb.VolumeSize)
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
	return openInitialized(path, f, &sb)
}

// OpenWALStore opens an existing store file WITHOUT recovering it.
// Call Recover() before any Read/Write to replay the WAL.
func OpenWALStore(path string) (*WALStore, error) {
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
	return openInitialized(path, f, &sb)
}

// openInitialized is the shared constructor body used by both Create
// and Open after the superblock is in place. It wires the WAL writer,
// the dirty map, and the group committer; it does NOT replay the WAL
// (that is Recover's job).
func openInitialized(path string, f *os.File, sb *superblock) (*WALStore, error) {
	dm := newDirtyMap(64) // 64 shards is plenty for Phase 07 demo workloads

	s := &WALStore{
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
	committer := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error { return f.Sync() },
		MaxDelay: 1 * time.Millisecond,
		MaxBatch: 64,
	})
	go committer.Run()
	s.committer = committer
	s.syncCache = committer.SyncCache
	s.syncDirectFrontierMetadata = func(data []byte) error {
		if _, err := f.WriteAt(data, 0); err != nil {
			return fmt.Errorf("storage: pwrite direct frontier superblock: %w", err)
		}
		if err := f.Sync(); err != nil {
			return fmt.Errorf("storage: fsync direct frontier superblock: %w", err)
		}
		return nil
	}
	s.writeSuperblockMetadata = func(data []byte) error {
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
	s.admission = newWALAdmission(walAdmissionConfig{
		MaxConcurrent: 64,
		SoftWatermark: 0.70,
		HardWatermark: 0.90,
		WALUsedFn: func() float64 {
			return s.wal.usedFraction()
		},
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
	return true, nil
}

func (s *WALStore) writeExtentUnlocked(lba uint32, data []byte) error {
	off := int64(s.extentBase + uint64(lba)*uint64(s.sb.BlockSize))
	if _, err := s.fd.WriteAt(data, off); err != nil {
		return fmt.Errorf("storage: write extent LBA %d: %w", lba, err)
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
	if highestLSN <= s.checkpointLSN {
		return nil
	}

	sbCopy := *s.sb
	sbCopy.WALCheckpointLSN = highestLSN
	sbCopy.WALHead = s.wal.logicalHeadValue()
	sbCopy.WALTail = s.wal.logicalTailValue()
	buf := newSimpleByteBuf()
	if _, err := sbCopy.writeTo(buf); err != nil {
		return fmt.Errorf("storage: encode superblock: %w", err)
	}
	checkpointWriteStart := time.Now()
	err := s.writeSuperblockMetadata(buf.bytes())
	if s.flusher != nil {
		s.flusher.instr.recordCheckpointWrite(len(buf.bytes()), time.Since(checkpointWriteStart), err)
	}
	if err != nil {
		return fmt.Errorf("storage: pwrite superblock: %w", err)
	}
	checkpointSyncStart := time.Now()
	err = s.syncSuperblockMetadata()
	if s.flusher != nil {
		s.flusher.instr.recordCheckpointSync(time.Since(checkpointSyncStart), err)
	}
	if err != nil {
		return fmt.Errorf("storage: fsync checkpoint superblock: %w", err)
	}
	s.checkpointLSN = highestLSN
	s.sb.WALCheckpointLSN = highestLSN
	s.sb.WALHead = sbCopy.WALHead
	s.sb.WALTail = sbCopy.WALTail
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

// Recover replays WAL entries past the last checkpoint into the dirty
// map. After Recover, the in-memory state matches what an aborted
// process would have left durably on disk.
//
// Idempotent: calling Recover twice on the same on-disk state yields
// identical results.
func (s *WALStore) Recover() (uint64, error) {
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errors.New("storage: Recover after Close")
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
	s.sb.WALHead = res.WALHead
	s.sb.WALTail = res.WALTail
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
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
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
	if s.admission != nil {
		if err := s.admission.Acquire(s.admissionTimeout); err != nil {
			return 0, fmt.Errorf("storage: WAL admission: %w", err)
		}
		defer s.admission.Release()
	}

	commitLockStart := time.Now()
	s.mu.Lock()
	s.instr.recordWriteCommitLockWait(time.Since(commitLockStart))
	if s.closed {
		s.mu.Unlock()
		return 0, errors.New("storage: Write after Close")
	}
	lsn := s.nextLSN

	entry := &walEntry{
		LSN:    lsn,
		Type:   walEntryWrite,
		LBA:    uint64(lba),
		Length: uint32(len(data)),
		Data:   data,
	}
	walRelOff, err := s.wal.append(entry)
	if err != nil {
		s.mu.Unlock()
		return 0, fmt.Errorf("storage: WAL append: %w", err)
	}
	dirtyStart := time.Now()
	s.dm.put(
		uint64(lba), walRelOff, lsn, uint32(len(data)),
		uint64(walEntryHeaderSize+len(data)),
	)
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
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
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

	if s.admission != nil {
		if err := s.admission.Acquire(s.admissionTimeout); err != nil {
			return nil, fmt.Errorf("storage: WAL admission: %w", err)
		}
		defer s.admission.Release()
	}

	commitLockStart := time.Now()
	s.mu.Lock()
	s.instr.recordWriteCommitLockWait(time.Since(commitLockStart))
	if s.closed {
		s.mu.Unlock()
		return nil, errors.New("storage: WriteBatch after Close")
	}
	firstLSN := s.nextLSN
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
	offsets, err := s.wal.appendBatch(entries)
	if err != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("storage: WAL batch append: %w", err)
	}
	dirtyStart := time.Now()
	for i, walRelOff := range offsets {
		s.dm.put(
			uint64(startLBA+uint32(i)), walRelOff, lsns[i], uint32(len(blocks[i])),
			uint64(walEntryHeaderSize+len(blocks[i])),
		)
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
	walRelOff, err := s.wal.append(entry)
	if err != nil {
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
			uint64(walEntryHeaderSize+len(payload)),
		)
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
func (s *WALStore) Read(lba uint32) ([]byte, error) {
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

	if walRelOff, dataOffset, _, _, ok := s.dm.get(uint64(lba)); ok {
		return s.readFromWAL(walRelOff, dataOffset)
	}
	return s.readFromExtent(lba)
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

func (s *WALStore) enableSingleReadMaterializationForTest(enabled bool) {
	s.singleReadMaterialization.Store(enabled)
}

func (s *WALStore) enableSharedRecordMaterializationForTest(enabled bool) {
	if enabled {
		s.singleReadMaterialization.Store(true)
	}
	s.sharedRecordMaterialization.Store(enabled)
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
	lock := &s.extentMu[uint64(lba)%uint64(len(s.extentMu))]
	lock.RLock()
	defer lock.RUnlock()
	data := make([]byte, s.sb.BlockSize)
	off := int64(s.extentBase + uint64(lba)*uint64(s.sb.BlockSize))
	if _, err := s.fd.ReadAt(data, off); err != nil {
		return nil, fmt.Errorf("storage: extent read LBA %d: %w", lba, err)
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
	targetHead := s.walHead
	targetDirectFrontier := s.pendingDirectFrontierLSN
	s.mu.RUnlock()

	if err := s.syncCache(); err != nil {
		return 0, fmt.Errorf("storage: group commit fsync: %w", err)
	}
	s.syncs.Add(1)

	s.mu.Lock()
	if targetDirectFrontier > s.checkpointLSN && targetDirectFrontier <= targetHead {
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
		if err := s.syncDirectFrontierMetadata(buf.bytes()); err != nil {
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
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
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
	if hadDirty {
		s.dm.compareAndDelete(uint64(lba), previousLSN)
	}
	return nil
}

// ApplyEntry writes a replicated block with the source's LSN rather
// than allocating a fresh one. Same durability semantics as Write
// (becomes durable on next Sync).
func (s *WALStore) ApplyEntry(lba uint32, data []byte, lsn uint64) error {
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	maxLBA := uint32(s.sb.VolumeSize / uint64(s.sb.BlockSize))
	if lba >= maxLBA {
		return fmt.Errorf("storage: apply LBA %d out of range", lba)
	}
	if len(data) != int(s.sb.BlockSize) {
		return fmt.Errorf("storage: apply data size %d != block size %d", len(data), s.sb.BlockSize)
	}
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return errors.New("storage: ApplyEntry after Close")
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
	walRelOff, err := s.wal.append(entry)
	if err != nil {
		return fmt.Errorf("storage: WAL append (apply): %w", err)
	}
	s.dm.put(
		uint64(lba), walRelOff, lsn, uint32(len(dataCopy)),
		uint64(walEntryHeaderSize+len(dataCopy)),
	)

	s.mu.Lock()
	defer s.mu.Unlock()
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
func (s *WALStore) AllBlocks() map[uint32][]byte {
	n := s.NumBlocks()
	out := make(map[uint32][]byte)
	zero := make([]byte, s.sb.BlockSize)
	for lba := uint32(0); lba < n; lba++ {
		data, err := s.Read(lba)
		if err != nil {
			continue
		}
		// Skip never-written / trimmed LBAs.
		if bytesAllZero(data, zero) {
			continue
		}
		out[lba] = data
	}
	return out
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
func (s *WALStore) Close() error {
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
	if s.fd != nil {
		// Persist current head/tail back into the superblock so a
		// subsequent OpenWALStore can find the WAL bounds without
		// relying solely on the defensive scan.
		s.sb.WALHead = s.wal.logicalHeadValue()
		s.sb.WALTail = s.wal.logicalTailValue()
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
