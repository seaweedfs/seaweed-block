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
	path      string
	fd        *os.File
	sb        *superblock
	wal       *walWriter
	dm        *dirtyMap
	committer *GroupCommitter
	flusher   *flusher      // background WAL→extent applier; nil if disabled
	admission *walAdmission // backpressure on WAL pressure; nil if disabled

	// admissionTimeout is the per-Write deadline for admission. If
	// the WAL stays above the hard watermark for longer than this,
	// the Write returns errWALFull. Defaults to 30s.
	admissionTimeout time.Duration

	mu        sync.RWMutex
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

	// recoveryRetentionLSNs is the operator-tunable retention window
	// past checkpointLSN: the recovery scan accepts fromLSN as long as
	// fromLSN > checkpointLSN - recoveryRetentionLSNs (G6 §1.A α).
	// Zero means strict checkpoint-driven recycle (pre-G6 behavior).
	// Operator sets via blockvolume's --wal-retention-lsns flag at
	// store construction. Stored in-memory only — NOT persisted to
	// the superblock; on restart the daemon re-applies the flag.
	//
	// Pinned by: INV-G6-RETENTION-POLICY-OPERATOR-VISIBLE.
	recoveryRetentionLSNs uint64

	syncs atomic.Uint64 // total fsync operations performed (test/diagnostic)
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
	wal := newWALWriter(f, sb.WALOffset, sb.WALSize, sb.WALHead, sb.WALTail)
	dm := newDirtyMap(64) // 64 shards is plenty for Phase 07 demo workloads

	s := &WALStore{
		path:          path,
		fd:            f,
		sb:            sb,
		wal:           wal,
		dm:            dm,
		extentBase:    sb.WALOffset + sb.WALSize,
		nextLSN:       sb.WALCheckpointLSN + 1,
		syncedLSN:     sb.WALCheckpointLSN,
		walTail:       sb.WALTail,
		walHead:       sb.WALHead,
		checkpointLSN: sb.WALCheckpointLSN,
	}
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

// writeExtent pwrites one block into the extent at lba's natural
// offset. Used by the flusher to drain WAL records into the extent.
// Caller is responsible for fsync (the flusher batches one fsync per
// flush cycle covering all writeExtent calls in that cycle).
func (s *WALStore) writeExtent(lba uint32, data []byte) error {
	off := int64(s.extentBase + uint64(lba)*uint64(s.sb.BlockSize))
	if _, err := s.fd.WriteAt(data, off); err != nil {
		return fmt.Errorf("storage: write extent LBA %d: %w", lba, err)
	}
	return nil
}

// persistCheckpoint advances the on-disk checkpoint LSN to highestLSN
// (after the flusher has confirmed the extent writes are fsync'd).
// Updates both the in-memory checkpointLSN and the on-disk
// superblock.WALCheckpointLSN. The superblock pwrite is itself
// unsynced — the next fsync (or close) flushes it.
//
// If we crash between the extent fsync and the superblock fsync,
// recovery sees the OLD checkpoint, replays the WAL records, and
// over-writes the extent with identical bytes. Wasteful but correct.
func (s *WALStore) persistCheckpoint(highestLSN uint64) error {
	s.mu.Lock()
	if highestLSN <= s.checkpointLSN {
		s.mu.Unlock()
		return nil
	}
	s.checkpointLSN = highestLSN
	s.sb.WALCheckpointLSN = highestLSN
	s.mu.Unlock()

	hdrBuf := make([]byte, superblockSize)
	if _, err := s.fd.ReadAt(hdrBuf, 0); err != nil {
		return fmt.Errorf("storage: read superblock for checkpoint: %w", err)
	}
	// Re-encode in place via writeTo onto a buffer, then pwrite at 0.
	// We use a small in-memory writer to avoid mucking with file
	// offset state under the WAL writer's nose.
	buf := newSimpleByteBuf()
	if _, err := s.sb.writeTo(buf); err != nil {
		return fmt.Errorf("storage: encode superblock: %w", err)
	}
	if _, err := s.fd.WriteAt(buf.bytes(), 0); err != nil {
		return fmt.Errorf("storage: pwrite superblock: %w", err)
	}
	return nil
}

// ResetForRebuild zeroes the extent, clears the WAL ring + dirty map,
// and persists a fresh-state superblock. After this call, every LBA
// reads as zero and the substrate is ready to receive a full base
// rebuild stream against a clean destination — INV-G7-REBUILD-
// SUBSTRATE-NO-STALE-EXPOSED. The receiver invokes this at the start
// of a new rebuild lineage so that `AllBlocks()`-filtered streams
// (which omit zero source blocks) leave correct content on LBAs the
// sender skips. Without it, a replica rejoining with a preserved
// walstore retained stale non-zero bytes at zero-LBAs and produced
// the G7 §2 #6 hardware-run mismatch pattern (run #3 evidence).
//
// Implementation: pwrite zeros over the extent, reset WAL writer,
// clear dirty map, zero the in-memory frontier + checkpoint, persist
// the cleared superblock, and fsync. After return, Read(lba) returns
// zeros for every LBA and frontier is 0.
//
// Performance: at 256 MiB extent + 64 MiB WAL the cost is one large
// pwrite + one fsync. On rotational media this is the long pole;
// SSDs handle it in well under a second.
func (s *WALStore) ResetForRebuild() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("storage: ResetForRebuild after Close")
	}

	// Zero the extent in 1 MiB chunks to keep allocator pressure low.
	const chunkSize = 1 << 20
	zeros := make([]byte, chunkSize)
	extentSize := int64(s.sb.VolumeSize)
	for written := int64(0); written < extentSize; {
		n := int64(chunkSize)
		if extentSize-written < n {
			n = extentSize - written
		}
		if _, err := s.fd.WriteAt(zeros[:n], int64(s.extentBase)+written); err != nil {
			return fmt.Errorf("storage: ResetForRebuild zero extent at %d: %w", written, err)
		}
		written += n
	}

	// Zero the WAL region too. Without this, on-disk WAL entry headers
	// from a prior session survive ResetForRebuild and `recoverWAL`'s
	// defensive scan could re-decode them at next process start —
	// dragging stale dirty-map state back over the freshly-zeroed
	// extent. INV-G7-REBUILD-SUBSTRATE-NO-STALE-EXPOSED holds across
	// restart only when the on-disk bytes match in-memory reset state.
	walRegionSize := int64(s.sb.WALSize)
	for written := int64(0); written < walRegionSize; {
		n := int64(chunkSize)
		if walRegionSize-written < n {
			n = walRegionSize - written
		}
		if _, err := s.fd.WriteAt(zeros[:n], int64(s.sb.WALOffset)+written); err != nil {
			return fmt.Errorf("storage: ResetForRebuild zero WAL region at %d: %w", written, err)
		}
		written += n
	}

	s.wal.reset()
	s.dm.clear()

	s.syncedLSN = 0
	s.walHead = 0
	s.walTail = 0
	s.nextLSN = 1
	s.checkpointLSN = 0

	s.sb.WALCheckpointLSN = 0
	s.sb.WALHead = 0
	s.sb.WALTail = 0

	hdr := newSimpleByteBuf()
	if _, err := s.sb.writeTo(hdr); err != nil {
		return fmt.Errorf("storage: ResetForRebuild encode superblock: %w", err)
	}
	if _, err := s.fd.WriteAt(hdr.bytes(), 0); err != nil {
		return fmt.Errorf("storage: ResetForRebuild pwrite superblock: %w", err)
	}
	if err := s.fd.Sync(); err != nil {
		return fmt.Errorf("storage: ResetForRebuild fsync: %w", err)
	}
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

// Recover replays WAL entries past the last checkpoint into the dirty
// map. After Recover, the in-memory state matches what an aborted
// process would have left durably on disk.
//
// Idempotent: calling Recover twice on the same on-disk state yields
// identical results.
func (s *WALStore) Recover() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errors.New("storage: Recover after Close")
	}
	s.dm.clear()
	res, err := recoverWAL(s.fd, s.sb, s.dm)
	if err != nil {
		return 0, err
	}
	if res.HighestLSN >= s.nextLSN {
		s.nextLSN = res.HighestLSN + 1
	}
	if res.HighestLSN > s.syncedLSN {
		s.syncedLSN = res.HighestLSN
	}
	if res.HighestLSN > s.walHead {
		s.walHead = res.HighestLSN
	}
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

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, errors.New("storage: Write after Close")
	}
	lsn := s.nextLSN
	s.nextLSN++
	s.mu.Unlock()

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
		return 0, fmt.Errorf("storage: WAL append: %w", err)
	}
	s.dm.put(uint64(lba), walRelOff, lsn, uint32(len(dataCopy)))

	s.mu.Lock()
	if lsn > s.walHead {
		s.walHead = lsn
	}
	if s.walTail == 0 {
		s.walTail = lsn
	}
	s.mu.Unlock()
	return lsn, nil
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

	if walRelOff, _, _, ok := s.dm.get(uint64(lba)); ok {
		return s.readFromWAL(walRelOff)
	}
	return s.readFromExtent(lba)
}

// readFromWAL pulls a single block out of a WAL Write/Trim entry
// previously deposited by Append. The walRelOff points at the start
// of the entry; data starts at walEntryHeaderSize bytes into it.
func (s *WALStore) readFromWAL(walRelOff uint64) ([]byte, error) {
	headerBuf := make([]byte, walEntryHeaderSize)
	absOff := int64(s.sb.WALOffset + walRelOff)
	if _, err := s.fd.ReadAt(headerBuf, absOff); err != nil {
		return nil, fmt.Errorf("storage: WAL read header: %w", err)
	}
	length := parseLengthFromHeader(headerBuf)
	if length == 0 {
		// Trim entry — return zeros, same as a never-written LBA.
		return make([]byte, s.sb.BlockSize), nil
	}
	data := make([]byte, length)
	if _, err := s.fd.ReadAt(data, absOff+int64(walEntryPrefixSize)); err != nil {
		return nil, fmt.Errorf("storage: WAL read data: %w", err)
	}
	// In the simple "one block per WAL write" case, length == blockSize.
	if uint32(len(data)) > s.sb.BlockSize {
		data = data[:s.sb.BlockSize]
	}
	return data, nil
}

func (s *WALStore) readFromExtent(lba uint32) ([]byte, error) {
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
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return 0, errors.New("storage: Sync after Close")
	}
	s.mu.RUnlock()

	if err := s.committer.SyncCache(); err != nil {
		return 0, fmt.Errorf("storage: group commit fsync: %w", err)
	}
	s.syncs.Add(1)

	s.mu.Lock()
	if s.walHead > s.syncedLSN {
		s.syncedLSN = s.walHead
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
// fromLSN > checkpointLSN - retentionLSNs (G6 §1.A α). Zero (the
// default) preserves pre-G6 strict checkpoint-driven recycle.
//
// Operator-tunable via blockvolume's --wal-retention-lsns flag.
// In-memory only; not persisted (re-applied on restart from CLI).
//
// Called by: DurableProvider construction path after walstore is
// opened, with the operator-supplied value from cmd/blockvolume.
// Owns: recoveryRetentionLSNs field under s.mu.
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if lsn >= s.nextLSN {
		s.nextLSN = lsn + 1
	}
	if lsn > s.walHead {
		s.walHead = lsn
	}
}

// AdvanceWALTail moves the retained-window tail forward. After this,
// recovery cases where the requested LSN is below newTail must
// escalate to rebuild.
func (s *WALStore) AdvanceWALTail(newTail uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if newTail > s.walTail {
		s.walTail = newTail
	}
}

// ApplyEntry writes a replicated block with the source's LSN rather
// than allocating a fresh one. Same durability semantics as Write
// (becomes durable on next Sync).
func (s *WALStore) ApplyEntry(lba uint32, data []byte, lsn uint64) error {
	maxLBA := uint32(s.sb.VolumeSize / uint64(s.sb.BlockSize))
	if lba >= maxLBA {
		return fmt.Errorf("storage: apply LBA %d out of range", lba)
	}
	if len(data) != int(s.sb.BlockSize) {
		return fmt.Errorf("storage: apply data size %d != block size %d", len(data), s.sb.BlockSize)
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
	s.dm.put(uint64(lba), walRelOff, lsn, uint32(len(dataCopy)))

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

// RecoveryMode reports walstore's recovery sub-mode (T4d-4 part A;
// T4c §I row 6). walstore's ScanLBAs emits per-LSN entries from the
// retained WAL — V2-faithful per-LSN replay.
func (s *WALStore) RecoveryMode() RecoveryMode {
	return RecoveryModeWALReplay
}

// AppliedLSNs returns a partial view of per-LBA applied LSN: only
// LBAs whose latest write is still in the WAL (not yet flushed to
// extent) are reported. Once an entry is flushed and the dirty-map
// entry is cleared, walstore loses per-LBA LSN tracking — the
// extent stores data only, not per-LBA LSN.
//
// PARTIAL-VIEW LIMITATION (kickoff §2.5 #3 caveat): for full per-LBA
// applied-LSN tracking, walstore would need a permanent per-LBA LSN
// map (substrate refactor, out of T4d-1 scope). The replica recovery
// apply gate (T4d-2) is the authoritative correctness boundary; this
// partial seed is defense-in-depth — it correctly stale-skips
// recovery entries for LBAs still in the WAL window, and falls back
// to "appliedLSN[LBA] = 0" semantics for flushed LBAs (which means
// recovery WILL apply them — acceptable when the gate's session-only
// tracking + live-lane updates fill in the gap during the session).
//
// Called by: T4d-2 replica recovery apply gate at session start.
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
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	// Stop the flusher first so it can't race the file close.
	// flusher.Stop performs one final best-effort flush so any
	// pending dirty entries get into the extent + checkpoint
	// before we release the file.
	if s.flusher != nil {
		s.flusher.Stop()
	}
	if s.committer != nil {
		s.committer.Stop()
	}
	if s.fd != nil {
		// Persist current head/tail back into the superblock so a
		// subsequent OpenWALStore can find the WAL bounds without
		// relying solely on the defensive scan.
		s.sb.WALHead = s.wal.logicalHeadValue()
		s.sb.WALTail = s.wal.logicalTailValue()
		if _, err := s.fd.Seek(0, 0); err == nil {
			_, _ = s.sb.writeTo(s.fd)
			_ = s.fd.Sync()
		}
		err := s.fd.Close()
		s.fd = nil
		return err
	}
	return nil
}

// Compile-time assertion: WALStore satisfies LogicalStorage.
var _ LogicalStorage = (*WALStore)(nil)
