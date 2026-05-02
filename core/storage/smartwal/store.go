package smartwal

import (
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// defaultWALSlots: 65536 slots × 32 bytes = 2 MiB ring. Plenty for
// the experimental demo workload; production sizing would consider
// the working-set size and per-LBA overwrite frequency.
const defaultWALSlots = 65536

// Store is the SmartWAL implementation of core/storage.LogicalStorage.
//
// File layout (one preallocated file):
//
//	[0          .. headerSize)                4KB header (magic, geometry)
//	[headerSize .. extentBase)                ring buffer (WALSlots × 32 bytes)
//	[extentBase .. extentBase + numBlocks*BS) extent (block-indexed)
//
// Read path: pread directly from extent. The ring is not consulted
// for reads — by construction the extent is current truth.
//
// Write path:
//   1. read existing extent block (kept in memory for rollback)
//   2. pwrite new data to extent
//   3. compute dataCRC32
//   4. append metadata record to ring (slot = LSN % capacity)
//   5. on append failure: rollback extent to saved old data
//
// Sync: one fsync of the file. Covers BOTH the extent pwrite and the
// ring append since they share a file. After Sync returns, all
// preceding writes' data + metadata are durable.
//
// Recovery:
//   1. scan ring, decode valid records, sort by LSN
//   2. compute last-writer-wins map (highest-LSN record per LBA)
//   3. for each surviving record, verify the extent block's CRC
//      against the record's dataCRC32; mismatch → skip (torn write)
//   4. set nextLSN = highest valid LSN + 1
//
// Concurrency: a single mutex guards bookkeeping (nextLSN, syncedLSN,
// counters); the ring has its own mutex for slot writes. Writes for
// the same LBA from different goroutines are NOT serialized at this
// layer — callers must order their own per-LBA writes.
type Store struct {
	path      string
	fd        *os.File
	hdr       *header
	ring      *ring
	committer *storage.GroupCommitter // batches concurrent Sync into one fsync

	extentBase int64

	mu        sync.RWMutex
	closed    bool
	nextLSN   uint64 // next LSN to assign
	syncedLSN uint64 // highest LSN known durable on disk
	walTail   uint64 // exposed S boundary
	walHead   uint64 // exposed H boundary

	syncs atomic.Uint64
}

// CreateStore initializes a fresh smartwal store at path. Fails if
// path already exists. Use OpenStore for the recovery path.
func CreateStore(path string, numBlocks uint32, blockSize int) (*Store, error) {
	return CreateStoreWithSlots(path, numBlocks, blockSize, defaultWALSlots)
}

// CreateStoreWithSlots is CreateStore with an explicit ring capacity.
// Larger rings tolerate more writes between full flushes; smaller
// rings recycle slots faster.
func CreateStoreWithSlots(path string, numBlocks uint32, blockSize int, walSlots uint64) (*Store, error) {
	if blockSize == 0 {
		blockSize = storage.DefaultBlockSize
	}
	if walSlots == 0 {
		walSlots = defaultWALSlots
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("smartwal: mkdir %s: %w", filepath.Dir(path), err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("smartwal: create %s: %w", path, err)
	}
	hdr, err := newHeader(uint32(blockSize), numBlocks, walSlots)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	hdr.CreatedAt = uint64(time.Now().UnixNano())

	totalSize := int64(headerSize) +
		int64(walSlots*recordSize) +
		int64(numBlocks)*int64(blockSize)
	if err := f.Truncate(totalSize); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("smartwal: preallocate %d bytes: %w", totalSize, err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("smartwal: seek to header: %w", err)
	}
	if err := hdr.writeTo(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("smartwal: fsync after create: %w", err)
	}
	return openInitialized(path, f, &hdr)
}

// OpenStore opens an existing store WITHOUT recovering it. Call
// Recover() before any Read/Write to rebuild the LSN counter and
// validate extent contents against the ring's metadata.
func OpenStore(path string) (*Store, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("smartwal: open %s: %w", path, err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("smartwal: seek to header: %w", err)
	}
	hdr, err := readHeader(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := hdr.validate(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return openInitialized(path, f, &hdr)
}

func openInitialized(path string, f *os.File, hdr *header) (*Store, error) {
	ringBase := int64(headerSize)
	extentBase := ringBase + int64(hdr.WALSlots)*recordSize
	r := newRing(f, ringBase, hdr.WALSlots)
	s := &Store{
		path:       path,
		fd:         f,
		hdr:        hdr,
		ring:       r,
		extentBase: extentBase,
		nextLSN:    1,
	}
	// Group committer batches concurrent Sync callers into one
	// fsync — same pattern WALStore uses. Without this, SmartWAL
	// scales poorly under concurrent durable writers because each
	// caller's Sync() does its own fd.Sync().
	s.committer = storage.NewGroupCommitter(storage.GroupCommitterConfig{
		SyncFunc: func() error { return f.Sync() },
		MaxDelay: 1 * time.Millisecond,
		MaxBatch: 64,
	})
	go s.committer.Run()
	return s, nil
}

// Recover rebuilds the in-memory LSN counter from the ring buffer.
// Idempotent: calling twice on the same on-disk state yields the
// same result.
func (s *Store) Recover() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errors.New("smartwal: Recover after Close")
	}
	frontier, err := runRecovery(s.ring, s.fd, s.extentBase, int(s.hdr.BlockSize))
	if err != nil {
		return 0, err
	}
	if frontier >= s.nextLSN {
		s.nextLSN = frontier + 1
	}
	if frontier > s.syncedLSN {
		s.syncedLSN = frontier
	}
	if frontier > s.walHead {
		s.walHead = frontier
	}
	if s.walTail == 0 && frontier > 0 {
		s.walTail = 1
	}
	return s.syncedLSN, nil
}

// Write writes one block to extent, then appends a metadata record.
// Failure-atomic: if the metadata append fails after the extent
// write, the extent is rolled back to its prior bytes so a later
// Sync cannot make untracked data durable.
func (s *Store) Write(lba uint32, data []byte) (uint64, error) {
	if lba >= s.hdr.NumBlocks {
		return 0, fmt.Errorf("smartwal: LBA %d out of range", lba)
	}
	if len(data) != int(s.hdr.BlockSize) {
		return 0, fmt.Errorf("smartwal: data size %d != block size %d", len(data), s.hdr.BlockSize)
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, errors.New("smartwal: Write after Close")
	}
	lsn := s.nextLSN
	s.nextLSN++
	s.mu.Unlock()

	if err := s.writeAt(lba, lsn, data, flagWrite); err != nil {
		return 0, err
	}
	s.bumpHead(lsn)
	return lsn, nil
}

// writeAt is the shared write path used by Write and ApplyEntry.
// Saves prior extent bytes, writes new bytes, computes CRC, appends
// to ring, and rolls back on append failure.
func (s *Store) writeAt(lba uint32, lsn uint64, data []byte, flags uint8) error {
	offset := s.extentBase + int64(lba)*int64(s.hdr.BlockSize)

	old := make([]byte, s.hdr.BlockSize)
	if _, err := s.fd.ReadAt(old, offset); err != nil {
		return fmt.Errorf("smartwal: read pre-write extent LBA %d: %w", lba, err)
	}
	if _, err := s.fd.WriteAt(data, offset); err != nil {
		return fmt.Errorf("smartwal: write extent LBA %d: %w", lba, err)
	}
	rec := record{
		LSN:       lsn,
		LBA:       lba,
		Flags:     flags,
		DataCRC32: crc32.ChecksumIEEE(data),
	}
	if err := s.ring.appendAt(lsn, rec); err != nil {
		// Rollback: restore prior extent bytes so an unsynced extent
		// write cannot become "untracked durable" via a later Sync.
		_, _ = s.fd.WriteAt(old, offset)
		return fmt.Errorf("smartwal: ring append (extent rolled back): %w", err)
	}
	return nil
}

// Read returns the current bytes at lba. SmartWAL reads always come
// from the extent — the ring is not consulted on the read path.
func (s *Store) Read(lba uint32) ([]byte, error) {
	if lba >= s.hdr.NumBlocks {
		return nil, fmt.Errorf("smartwal: LBA %d out of range", lba)
	}
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, errors.New("smartwal: Read after Close")
	}
	s.mu.RUnlock()
	data := make([]byte, s.hdr.BlockSize)
	off := s.extentBase + int64(lba)*int64(s.hdr.BlockSize)
	if _, err := s.fd.ReadAt(data, off); err != nil {
		return nil, fmt.Errorf("smartwal: read extent LBA %d: %w", lba, err)
	}
	return data, nil
}

// Sync forces all in-flight writes to durable storage. One fsync
// covers both the extent pwrites and the ring appends because they
// share a file.
func (s *Store) Sync() (uint64, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return 0, errors.New("smartwal: Sync after Close")
	}
	s.mu.RUnlock()
	if err := s.committer.SyncCache(); err != nil {
		return 0, fmt.Errorf("smartwal: group commit fsync: %w", err)
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

// Boundaries returns the live R/S/H boundaries.
func (s *Store) Boundaries() (R, S, H uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.syncedLSN, s.walTail, s.walHead
}

// NextLSN returns the LSN that will be assigned to the next Write.
func (s *Store) NextLSN() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextLSN
}

// NumBlocks returns the addressable block count.
func (s *Store) NumBlocks() uint32 { return s.hdr.NumBlocks }

// BlockSize returns the IO unit size in bytes.
func (s *Store) BlockSize() int { return int(s.hdr.BlockSize) }

// AdvanceFrontier bumps the recorded frontier without writing data.
func (s *Store) AdvanceFrontier(lsn uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lsn >= s.nextLSN {
		s.nextLSN = lsn + 1
	}
	if lsn > s.walHead {
		s.walHead = lsn
	}
}

// AdvanceWALTail moves the retained-window tail forward.
func (s *Store) AdvanceWALTail(newTail uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if newTail > s.walTail {
		s.walTail = newTail
	}
}

// WriteExtentDirect installs a base block directly into the extent
// without going through the WAL ring append path — INV-RECV-BITMAP-
// CORE (§6.10). The receiver's per-session bitmap is the sole arbiter
// of BASE-vs-WAL conflict at this LBA; substrate-level WAL replay /
// stale-skip is intentionally bypassed.
//
// No LSN is recorded; the ring is NOT touched. nextLSN / walHead are
// NOT advanced. The recovery layer pairs this with AdvanceFrontier
// (targetLSN) at MarkBaseComplete to keep post-rebuild frontier
// reporting honest.
//
// Durability follows the same rule as Write: bytes become durable
// only on the next successful Sync.
func (s *Store) WriteExtentDirect(lba uint32, data []byte) error {
	if lba >= s.hdr.NumBlocks {
		return fmt.Errorf("smartwal: WriteExtentDirect LBA %d out of range", lba)
	}
	if len(data) != int(s.hdr.BlockSize) {
		return fmt.Errorf("smartwal: WriteExtentDirect data size %d != block size %d", len(data), s.hdr.BlockSize)
	}
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return errors.New("smartwal: WriteExtentDirect after Close")
	}
	s.mu.RUnlock()
	offset := s.extentBase + int64(lba)*int64(s.hdr.BlockSize)
	if _, err := s.fd.WriteAt(data, offset); err != nil {
		return fmt.Errorf("smartwal: WriteExtentDirect extent LBA %d: %w", lba, err)
	}
	return nil
}

// ApplyEntry writes a replicated block with the source's LSN.
func (s *Store) ApplyEntry(lba uint32, data []byte, lsn uint64) error {
	if lba >= s.hdr.NumBlocks {
		return fmt.Errorf("smartwal: apply LBA %d out of range", lba)
	}
	if len(data) != int(s.hdr.BlockSize) {
		return fmt.Errorf("smartwal: apply data size %d != block size %d", len(data), s.hdr.BlockSize)
	}
	if err := s.writeAt(lba, lsn, data, flagWrite); err != nil {
		return err
	}
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

// RecoveryMode reports smartwal's recovery sub-mode (T4d-4 part A;
// T4c §I row 6). smartwal's ScanLBAs emits per-LBA last-writer-wins
// with scan-time LSN — state-convergence semantics.
func (s *Store) RecoveryMode() storage.RecoveryMode {
	return storage.RecoveryModeStateConvergence
}

// AppliedLSNs returns per-LBA latest-applied-LSN derived from the
// ring's valid records. smartwal's ring is capacity-bounded: records
// older than (head - capacity) have been overwritten, so the returned
// map is "best-effort within retention" — LBAs whose latest write has
// rolled out of the ring are not reported.
//
// PARTIAL-VIEW NOTE (kickoff §2.5 #3 caveat): smartwal's ring
// retention is typically larger than walstore's WAL window, so this
// view is more complete than walstore's `AppliedLSNs()`. Still
// defense-in-depth — the apply gate (T4d-2) is the authoritative
// correctness boundary.
//
// Called by: T4d-2 replica recovery apply gate at session start.
// Owns: per-call snapshot from `ring.scanValid()` reduced per-LBA
// last-writer-wins.
// Borrows: nothing (returned map is fresh and caller-owned).
func (s *Store) AppliedLSNs() (map[uint32]uint64, error) {
	records, err := s.ring.scanValid()
	if err != nil {
		return nil, fmt.Errorf("smartwal: AppliedLSNs ring scan: %w", err)
	}
	out := make(map[uint32]uint64, len(records))
	for _, rec := range records {
		if existing, ok := out[rec.LBA]; !ok || rec.LSN > existing {
			out[rec.LBA] = rec.LSN
		}
	}
	return out, nil
}

// AllBlocks snapshots every LBA's current bytes by reading directly
// from the extent. Linear scan — N reads — but in practice the
// rebuild server is the only caller and runs once per session.
func (s *Store) AllBlocks() map[uint32][]byte {
	out := make(map[uint32][]byte, s.hdr.NumBlocks)
	for lba := uint32(0); lba < s.hdr.NumBlocks; lba++ {
		data, err := s.Read(lba)
		if err != nil {
			continue
		}
		// Only include LBAs with non-zero content — preserves the
		// "snapshot of dirty bits" semantics callers expect.
		nonZero := false
		for _, b := range data {
			if b != 0 {
				nonZero = true
				break
			}
		}
		if nonZero {
			out[lba] = data
		}
	}
	return out
}

// Close fsyncs and releases the file. Idempotent.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	if s.committer != nil {
		s.committer.Stop()
	}
	if s.fd != nil {
		_ = s.fd.Sync()
		err := s.fd.Close()
		s.fd = nil
		return err
	}
	return nil
}

// bumpHead advances the in-memory walHead boundary under lock. Called
// after a successful Write/ApplyEntry so subsequent Boundaries()
// readers see the new head.
func (s *Store) bumpHead(lsn uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lsn > s.walHead {
		s.walHead = lsn
	}
	if s.walTail == 0 {
		s.walTail = lsn
	}
}

// SyncCount returns the total number of fsync operations performed.
// Diagnostic only.
func (s *Store) SyncCount() uint64 { return s.syncs.Load() }

// Compile-time assertion: Store satisfies the LogicalStorage contract.
var _ storage.LogicalStorage = (*Store)(nil)
