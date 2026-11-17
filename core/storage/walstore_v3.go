package storage

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

// Impl-3 internals of the real WALStore (format bump v3, DESIGN r15
// section 6; plan rows 5 and 5b). A *WALStore whose v3 field is set runs
// the v3 format: its descriptor publisher (row 3), its WAL writer and scan
// (row 4), and the row-5 recovery (R < W*, the exact drop fence, head = E,
// admission). Every other piece of the store is shared with v2: the group
// committer, the dirty map, the flusher's extent writes, the syncFile leaf,
// fileSyncMu and the sticky fault.
//
// Dark until activation (plan row 12b): only createWALStoreV3ForTest and
// openWALStoreV3ForTest build a v3 store; Create and Open stay v2.
//
// Not yet v3 (refused with errV3NotYet until their rows): WriteBatch, the
// install paths (ApplyEntry, WriteExtentDirect, AdvanceFrontier; rows 9 to
// 11) and the checkpoint/tail advance, which needs the row-7 map candidate.
type walStoreV3State struct {
	pub *descriptorPublisherV3
	w   *walWriterV3

	appended atomic.Uint64 // highest LSN pwritten to the WAL
	wack     atomic.Uint64 // highest acknowledged LSN (Sync completed)

	// syncMu is the group-commit leader role for the v3-only publications
	// that run outside the committer (refreshW, SetWMode).
	syncMu sync.Mutex

	// mapCandidate, when set by plan row 7, prepares the map candidate a
	// checkpoint advance needs (DESIGN 9.1). Until then the v3 checkpoint
	// and tail do not move: coverage is never fabricated.
	mapCandidate func(checkpoint uint64) (func(next *descriptorV3) error, error)

	// Test hooks (never set in production). testBeforeSlotWrite runs inside
	// the leaf just before a descriptor slot pwrite; a non-nil error fails
	// that write. testLeaderWaiting runs in a committer batch just before it
	// takes the leader role.
	testBeforeSlotWrite func() error
	testLeaderWaiting   func()
	// testSetWModeHoldingRole runs in SetWMode after it has taken the leader
	// role and before it publishes the new mode.
	testSetWModeHoldingRole func()
}

var errV3NotYet = errors.New("storage: operation not yet supported on a v3 store (later plan row)")

// testSkipFenceOnRecoverV3 is a test-only mutant switch for the G1 control
// (it proves the schedule resurrects without the fence). Never set outside
// tests.
var testSkipFenceOnRecoverV3 bool

func (s *WALStore) latchV3(err error) {
	if err != nil {
		s.ioFault.CompareAndSwap(nil, NewSubstrateIOFailure(err,
			"v3 WAL append or descriptor publication failed; the open store refuses further Syncs, writes and checkpoints"))
	}
}

// createWALStoreV3ForTest creates a v3 file (slot A at generation 1) and
// returns it opened and recovered. It is the only v3 creator before
// activation.
func createWALStoreV3ForTest(path string, numBlocks uint32, blockSize uint32, walSize uint64, mode WMode) (*WALStore, error) {
	return createWALStoreV3AtCursorForTest(path, numBlocks, blockSize, walSize, mode, 0)
}

// createWALStoreV3AtCursorForTest is createWALStoreV3ForTest with the WAL's
// logical cursor (tail = head) starting at cursor, to exercise the last
// representable cursors.
func createWALStoreV3AtCursorForTest(path string, numBlocks uint32, blockSize uint32, walSize uint64, mode WMode, cursor uint64) (*WALStore, error) {
	d := descriptorV3{
		ImplKind: ImplKindWALStore, ImplVersion: walStoreImplVersion3,
		VolumeSize: uint64(numBlocks) * uint64(blockSize), ExtentSize: 64 * 1024, BlockSize: blockSize,
		WALOffset: walOffsetV3, WALSize: walSize, CreatedAt: 1, SlotGen: 1,
		MapFormatID: mapFormatIDV1, MapGeneration: 1, WMode: mode,
		RetainedTail: cursor, RetainedHead: cursor,
	}
	for _, b := range [][]byte{d.UUID[:], d.Nonce[:], d.MapLineage[:]} {
		if _, err := rand.Read(b); err != nil {
			return nil, err
		}
	}
	if err := d.validate(nil); err != nil {
		return nil, err
	}
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	err = fd.Truncate(int64(d.WALOffset + d.WALSize + d.VolumeSize))
	if err == nil {
		_, err = fd.WriteAt(d.encode(), slotAOffsetV3)
	}
	if err == nil {
		err = fd.Sync()
	}
	if cerr := fd.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, err
	}
	s, err := openWALStoreV3ForTest(path)
	if err != nil {
		return nil, err
	}
	if _, err := s.Recover(); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

// openWALStoreV3ForTest opens a v3 file WITHOUT recovering it, like
// OpenWALStore: call Recover before any Read or Write.
func openWALStoreV3ForTest(path string) (*WALStore, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", path, err)
	}
	a := make([]byte, slotSizeV3)
	b := make([]byte, slotSizeV3)
	_, errA := f.ReadAt(a, slotAOffsetV3)
	_, errB := f.ReadAt(b, slotBOffsetV3)
	if err := errors.Join(errA, errB); err != nil {
		_ = f.Close()
		return nil, err
	}
	d, id, err := selectSlotV3(a, b, nil)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	var other *descriptorV3
	otherRaw := b
	if id == slotB {
		otherRaw = a
	}
	if od, oerr := decodeSlotV3(otherRaw, nil); oerr == nil && od.MigrationPhase == MigrationNormal {
		other = &od
	}
	// The shared store reads its geometry from sb; WAL bounds and the
	// checkpoint come from the selected descriptor.
	sb := superblock{
		Version: superblockVersionV3, UUID: d.UUID, VolumeSize: d.VolumeSize,
		ExtentSize: d.ExtentSize, BlockSize: d.BlockSize, WALOffset: d.WALOffset, WALSize: d.WALSize,
		WALHead: d.RetainedHead, WALTail: d.RetainedTail, WALCheckpointLSN: d.CheckpointLSN,
		CreatedAt: d.CreatedAt, ImplKind: d.ImplKind, ImplVersion: d.ImplVersion,
	}
	copy(sb.Magic[:], superblockMagic)
	var initErr error
	s, err := openInitializedInternal(path, f, &sb, nil, func(s *WALStore) {
		v := &walStoreV3State{}
		v.pub = newDescriptorPublisherV3(d, id, other, descriptorIOV3{
			writeSlot: func(off int64, buf []byte) error {
				if h := s.v3.testBeforeSlotWrite; h != nil {
					if err := h(); err != nil {
						return err
					}
				}
				n, err := f.WriteAt(buf, off)
				if err == nil && n != len(buf) {
					err = fmt.Errorf("storage: v3 slot short write %d of %d", n, len(buf))
				}
				return err
			},
			fsync:    f.Sync,
			syncFile: s.syncFile,
			sticky:   s.stickyIOFault,
			latch:    s.latchV3,
		})
		w, err := newWALWriterV3(f, v.pub)
		if err != nil {
			initErr = err
			return
		}
		v.w = w
		v.wack.Store(d.W)
		s.v3 = v
	})
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if initErr != nil {
		_ = s.Close()
		return nil, initErr
	}
	return s, nil
}

// recoverV3 is Recover for a v3 store (DESIGN 6.5, 6.6). The caller holds
// captureMu, lifecycleMu and s.mu, as v2 Recover does.
func (s *WALStore) recoverV3() (uint64, error) {
	v := s.v3
	d := v.pub.current()
	s.dm.clear()
	scan, err := scanWALV3(s.fd, d)
	if err != nil {
		return 0, err
	}
	recs := make([]trustedRecordV3, 0, len(scan.Records))
	for _, r := range scan.Records {
		last := r.Entry.LSN
		if r.Entry.Type == walEntryWriteBatch {
			last += r.Entry.BatchCount - 1
		}
		recs = append(recs, trustedRecordV3{FirstLSN: r.Entry.LSN, LastLSN: last, End: r.End})
	}
	o, err := decideRecoveryV3(d, recs, scan.MaxDurableLSN)
	if err != nil {
		return 0, err // R < W*: fail closed (wal_corrupt)
	}
	if o.Installing {
		return 0, installingErrorV3(o)
	}
	// Replay the records <= R into the dirty map (last writer wins); the
	// rest are dropped and fenced below.
	for _, r := range scan.Records {
		e := r.Entry
		if e.LSN > o.R {
			break
		}
		if e.Type != walEntryWrite {
			s.dm.clear()
			return 0, fmt.Errorf("%w: replay of v3 record type %#x", errV3NotYet, e.Type)
		}
		size := uint64(walEntryHeaderSizeV3 + len(e.Data))
		phys := (r.End - size) % d.WALSize
		s.dm.putAtIfNewer(e.LBA, phys, 0, e.LSN, d.BlockSize)
	}
	// The drop fence, then the recovered head, then the writer and admission.
	floor, sameNonce := v.pub.reuseFloor()
	if err := s.fenceV3(o.E, floor, d.WALSize, sameNonce, d.WALOffset); err != nil {
		s.dm.clear()
		return 0, err
	}
	if _, err := v.pub.publish(nil, nil, func(n *descriptorV3) error {
		n.RetainedHead = o.E
		return nil
	}); err != nil {
		s.dm.clear()
		return 0, err
	}
	restore := scan
	restore.End = o.E
	if err := v.w.restoreScanV3(restore); err != nil {
		s.dm.clear()
		return 0, err
	}
	s.checkpointLSN = d.CheckpointLSN
	s.nextLSN = o.R + 1
	s.walHead, s.syncedLSN = o.R, o.R
	if s.walTail == 0 && o.R > 0 {
		s.walTail = 1
	}
	v.appended.Store(o.R)
	v.wack.Store(max(v.wack.Load(), o.R))
	return s.syncedLSN, nil
}

// fenceV3 zeroes exactly the fence ranges and fsyncs them through the leaf
// before any admission (DESIGN 6.6). A write error latches inside the leaf.
func (s *WALStore) fenceV3(E, floor, walSize uint64, sameNonce bool, walOffset uint64) error {
	ranges, err := fenceRangesV3(E, floor, walSize, sameNonce)
	if err != nil {
		return err
	}
	if len(ranges) == 0 || testSkipFenceOnRecoverV3 {
		return nil
	}
	return s.syncFile(func() error {
		const chunk = 64 * 1024
		zero := make([]byte, chunk)
		for _, r := range ranges {
			for done := uint64(0); done < r.Len; {
				n := min(r.Len-done, chunk)
				if _, err := s.fd.WriteAt(zero[:n], int64(walOffset+r.Off+done)); err != nil {
					err = fmt.Errorf("storage: v3 drop fence: %w", err)
					s.latchV3(err)
					return err
				}
				done += n
			}
		}
		return nil
	}, s.fd.Sync)
}

// appendWriteV3 is the v3 body of Write's append, under s.mu. DurableLSN is
// the acknowledged W at append time; a pwrite failure latches the store.
func (s *WALStore) appendWriteV3(lba uint32, data []byte, lsn uint64) (uint64, error) {
	e := walEntryV3{LSN: lsn, Type: walEntryWrite, LBA: uint64(lba), Length: uint32(len(data)), Data: data}
	phys, err := s.v3.w.append(e, s.v3.wack.Load())
	if err != nil {
		if errors.Is(err, errWALPwrite) {
			s.latchV3(err)
		}
		return 0, err
	}
	s.v3.appended.Store(lsn)
	return phys, nil
}

// syncWALBatchV3 is the committer leader's batch for a v3 store (DESIGN 6.2,
// 6.3). The cut H is every record pwritten before the leader starts.
func (s *WALStore) syncWALBatchV3() error {
	if err := s.stickyIOFault(); err != nil {
		return err
	}
	cut := s.walMutation.Load()
	if !s.bypassSyncDedup.Load() && s.chunk.Load() == nil && cut <= s.durableMutation.Load() {
		return nil
	}
	v := s.v3
	// The leader role: SetWMode holds it while it changes the mode, so no
	// batch acknowledges under a stale mode selection (the mode is read
	// only after the role is taken).
	if h := v.testLeaderWaiting; h != nil {
		h()
	}
	v.syncMu.Lock()
	defer v.syncMu.Unlock()
	H, prev := v.appended.Load(), v.wack.Load()
	switch v.pub.current().WMode {
	case WModeStrict:
		// A: fsync the WAL, then publish W = H with its own fsync.
		if err := s.syncFile(nil, func() error { return s.syncWALFile() }); err != nil {
			return err
		}
		if _, err := v.pub.publish(nil, nil, func(n *descriptorV3) error { n.W = H; return nil }); err != nil {
			return err
		}
	default:
		// C: the fallback slot carries the PREVIOUS batch's acknowledged
		// W, and the publication's one fsync covers this batch's records
		// and the slot. The slot never names an LSN beyond a completed fsync.
		s.walSyncCalls.Add(1)
		if _, err := v.pub.publish(nil, nil, func(n *descriptorV3) error { n.W = prev; return nil }); err != nil {
			return err
		}
	}
	// The acknowledgement boundary (codex01 row 5 P1): a queued append may
	// have failed and latched the store fault while this batch's slot I/O
	// was in flight. Recheck under the store lock, which that append holds
	// until it has latched, before acknowledging anything.
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.stickyIOFault(); err != nil {
		return err
	}
	if H > prev {
		v.wack.Store(H)
	}
	s.durableMutation.Store(cut)
	return nil
}

// refreshW is the mode-C idle refresh (DESIGN 6.2): publish W = the
// acknowledged W when the slot lags it.
func (s *WALStore) refreshW() error {
	v := s.v3
	if v == nil {
		return errV3NotYet
	}
	v.syncMu.Lock()
	defer v.syncMu.Unlock()
	wack := v.wack.Load()
	if wack <= v.pub.current().W {
		return nil
	}
	_, err := v.pub.publish(nil, nil, func(n *descriptorV3) error { n.W = wack; return nil })
	return err
}

// SetWMode changes the W mode (DESIGN 6.4). It runs a Sync first, so every
// write admitted before it is acknowledged under the old mode, then
// publishes the new mode with W = the actual acknowledged W while holding
// the v3 leader role. Every later batch uses the new mode.
func (s *WALStore) SetWMode(m WMode) error {
	v := s.v3
	if v == nil {
		return errV3NotYet
	}
	if _, err := s.Sync(); err != nil {
		return err
	}
	v.syncMu.Lock()
	defer v.syncMu.Unlock()
	if h := v.testSetWModeHoldingRole; h != nil {
		h()
	}
	wack := v.wack.Load()
	_, err := v.pub.publish(nil, nil, func(n *descriptorV3) error {
		n.WMode, n.W = m, wack
		return nil
	})
	return err
}

// readFromWALV3 reads one block of a v3 write record at a WAL offset.
func (s *WALStore) readFromWALV3(walRelOff uint64) ([]byte, error) {
	absOff := int64(s.sb.WALOffset + walRelOff)
	header := make([]byte, walEntryPrefixSizeV3)
	if _, err := s.fd.ReadAt(header, absOff); err != nil {
		return nil, fmt.Errorf("storage: v3 WAL read header: %w", err)
	}
	if header[16] != walEntryWrite || binary.LittleEndian.Uint32(header[26:30]) != s.sb.BlockSize {
		return nil, fmt.Errorf("storage: v3 WAL read: unexpected record type %#x", header[16])
	}
	data := make([]byte, s.sb.BlockSize)
	if _, err := s.fd.ReadAt(data, absOff+int64(walEntryPrefixSizeV3)); err != nil {
		return nil, fmt.Errorf("storage: v3 WAL read data: %w", err)
	}
	return data, nil
}

// readDirtyRecordV3 is the flusher's record read for a v3 store: it decodes
// and validates the whole record before its data may reach the extent.
func (f *flusher) readDirtyRecordV3(e snapshotEntry) ([]byte, uint64, error) {
	store := f.store
	d := store.v3.pub.current()
	absOff := int64(store.sb.WALOffset + e.WALOffset)
	size := uint64(walEntryHeaderSizeV3) + uint64(store.sb.BlockSize)
	if e.WALOffset+size > store.sb.WALSize {
		return nil, 0, fmt.Errorf("flusher: v3 record LBA %d offset %d crosses the ring end", e.LBA, e.WALOffset)
	}
	full := make([]byte, size)
	if _, err := store.fd.ReadAt(full, absOff); err != nil {
		return nil, 0, fmt.Errorf("flusher: read v3 WAL record at %d: %w", e.WALOffset, err)
	}
	entry, err := decodeWALEntryV3(full, d.BlockSize, d.VolumeSize/uint64(d.BlockSize))
	if err != nil {
		return nil, 0, fmt.Errorf("flusher: invalid v3 WAL record LBA %d offset %d: %w", e.LBA, e.WALOffset, err)
	}
	if entry.Type != walEntryWrite || entry.LSN != e.LSN || entry.LBA != e.LBA || entry.Nonce != d.Nonce ||
		e.DataOffset != 0 || e.Length != store.sb.BlockSize {
		return nil, 0, fmt.Errorf("flusher: v3 WAL slot mismatch LBA %d offset %d dirty LSN %d record LSN %d",
			e.LBA, e.WALOffset, e.LSN, entry.LSN)
	}
	return entry.Data, size, nil
}

// persistCheckpointV3 is the v3 checkpoint/tail publication. It goes
// through the row-3 publisher with the row-7 map candidate; without one,
// the checkpoint and tail do not move (no coverage is fabricated) and the
// flushed bytes stay retained in the WAL.
func (s *WALStore) persistCheckpointV3(highestLSN uint64) error {
	v := s.v3
	if v.mapCandidate == nil {
		return nil
	}
	mutateMap, err := v.mapCandidate(highestLSN)
	if err != nil {
		return err
	}
	d, err := v.pub.publish(&s.mu, nil, func(n *descriptorV3) error {
		n.CheckpointLSN = highestLSN
		return mutateMap(n)
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.checkpointLSN = d.CheckpointLSN
	s.mu.Unlock()
	return nil
}
