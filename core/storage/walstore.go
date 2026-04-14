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
	committer *groupCommitter

	mu        sync.RWMutex
	closed    bool
	nextLSN   uint64 // next LSN to assign to a write
	syncedLSN uint64 // highest LSN durably present in extent OR WAL after fsync
	walTail   uint64 // exposed S boundary (oldest retained LSN)
	walHead   uint64 // exposed H boundary (newest written LSN)

	// extentBase is the absolute file offset where the extent region
	// begins; cached for fast read/write.
	extentBase uint64

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
		BlockSize:  uint32(blockSize),
		ExtentSize: uint32(blockSize), // one block per extent slot keeps math trivial
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
		path:       path,
		fd:         f,
		sb:         sb,
		wal:        wal,
		dm:         dm,
		extentBase: sb.WALOffset + sb.WALSize,
		nextLSN:    sb.WALCheckpointLSN + 1,
		syncedLSN:  sb.WALCheckpointLSN,
		walTail:    sb.WALTail,
		walHead:    sb.WALHead,
	}
	if s.nextLSN < 1 {
		s.nextLSN = 1
	}
	committer := newGroupCommitter(groupCommitterConfig{
		SyncFunc: func() error { return f.Sync() },
		MaxDelay: 1 * time.Millisecond,
		MaxBatch: 64,
	})
	go committer.run()
	s.committer = committer
	return s, nil
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

// AllBlocks snapshots every dirty LBA's current bytes. Used by the
// rebuild server to enumerate what to ship. Clean LBAs (read from
// extent) are NOT included — the caller knows the clean tail by
// asking NumBlocks() and reading the missing ones if it needs them.
func (s *WALStore) AllBlocks() map[uint32][]byte {
	out := make(map[uint32][]byte)
	for _, e := range s.dm.snapshot() {
		data, err := s.readFromWAL(e.WALOffset)
		if err != nil {
			continue
		}
		out[uint32(e.LBA)] = data
	}
	return out
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
