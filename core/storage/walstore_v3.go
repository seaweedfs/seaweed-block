package storage

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// walStoreV3 is the dark impl-3 store core (format bump v3, DESIGN r15
// section 6; plan row 5). It composes the row-3 descriptor publisher, the
// row-4 WAL writer and scan, and the row-5 recovery decisions:
//
//   - open: select a slot, scan, decide R < W*, replay records <= R, fence
//     [E, floor+WALSize) exactly, publish retainedHead = E, restore the
//     writer to E, and only then admit appends;
//   - Sync in mode C (default: the slot carries the previous batch's W and
//     one fsync covers the WAL and the slot) or mode A (WAL fsync, then a
//     slot publication with W = H and its own fsync);
//   - the idle refresh, SetWMode, and the store-global fault latch for
//     append and publication failures.
//
// It is reachable only from tests until activation (plan row 12b). The
// map, checkpoint and extent flush are later rows: this core keeps the
// replayed and written blocks in memory for its read path.
type walStoreV3 struct {
	fd *os.File

	fileSyncMu sync.Mutex
	ioFault    atomic.Pointer[error] // store-global sticky fault

	mu       sync.Mutex // the WALStore.mu analogue
	nextLSN  uint64
	appended uint64 // highest LSN appended
	wack     uint64 // highest acknowledged LSN
	data     map[uint64][]byte
	admitted bool

	syncMu sync.Mutex // group-commit leader role; SetWMode takes it too

	pub *descriptorPublisherV3
	w   *walWriterV3

	// testBeforeFence, when set, runs just before the drop fence writes.
	testBeforeFence func()
}

// testSkipFenceOnRecoverV3 is a test-only mutant switch for the G1 control
// (it proves the schedule resurrects without the fence). Never set outside
// tests.
var testSkipFenceOnRecoverV3 bool

var errStoreNotAdmittedV3 = errors.New("storage: v3 store not admitted (recovery incomplete)")

func (s *walStoreV3) sticky() error {
	if e := s.ioFault.Load(); e != nil {
		return *e
	}
	return nil
}

func (s *walStoreV3) latch(err error) {
	if err != nil {
		s.ioFault.CompareAndSwap(nil, &err)
	}
}

// syncFile is the leaf: one optional pwrite and one fsync under fileSyncMu,
// refused when a fault is latched; an fsync failure latches before unlock.
func (s *walStoreV3) syncFile(write, sync func() error) error {
	s.fileSyncMu.Lock()
	defer s.fileSyncMu.Unlock()
	if err := s.sticky(); err != nil {
		return err
	}
	if write != nil {
		if err := write(); err != nil {
			return err
		}
	}
	if err := sync(); err != nil {
		err = fmt.Errorf("%w: %w", ErrFsyncFailed, err)
		s.latch(err)
		return err
	}
	return nil
}

func (s *walStoreV3) descriptorIO() descriptorIOV3 {
	return descriptorIOV3{
		writeSlot: func(off int64, b []byte) error {
			n, err := s.fd.WriteAt(b, off)
			if err == nil && n != len(b) {
				err = io.ErrShortWrite
			}
			return err
		},
		fsync:    s.fd.Sync,
		syncFile: s.syncFile,
		sticky:   s.sticky,
		latch:    s.latch,
	}
}

// createWALStoreV3ForTest creates a v3 file with slot A at generation 1 and
// opens it. It is the only constructor of a v3 store before activation.
func createWALStoreV3ForTest(path string, numBlocks uint32, blockSize uint32, walSize uint64, mode WMode) (*walStoreV3, error) {
	d := descriptorV3{
		ImplKind: ImplKindWALStore, ImplVersion: walStoreImplVersion3,
		VolumeSize: uint64(numBlocks) * uint64(blockSize), ExtentSize: 64 * 1024, BlockSize: blockSize,
		WALOffset: walOffsetV3, WALSize: walSize, CreatedAt: 1, SlotGen: 1,
		MapFormatID: mapFormatIDV1, MapGeneration: 1, WMode: mode,
	}
	if _, err := rand.Read(d.UUID[:]); err != nil {
		return nil, err
	}
	if _, err := rand.Read(d.Nonce[:]); err != nil {
		return nil, err
	}
	if _, err := rand.Read(d.MapLineage[:]); err != nil {
		return nil, err
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
	return openWALStoreV3(path)
}

// openWALStoreV3 opens and recovers a v3 file (DESIGN 6.5, 6.6). An
// installing descriptor returns the typed not-ready error (rows 9 to 11
// own the install paths).
func openWALStoreV3(path string) (*walStoreV3, error) {
	fd, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	s, err := recoverWALStoreV3(fd)
	if err != nil {
		_ = fd.Close()
		return nil, err
	}
	return s, nil
}

func recoverWALStoreV3(fd *os.File) (*walStoreV3, error) {
	a := make([]byte, slotSizeV3)
	b := make([]byte, slotSizeV3)
	if _, err := fd.ReadAt(a, slotAOffsetV3); err != nil {
		return nil, err
	}
	if _, err := fd.ReadAt(b, slotBOffsetV3); err != nil {
		return nil, err
	}
	d, id, err := selectSlotV3(a, b, nil)
	if err != nil {
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
	s := &walStoreV3{fd: fd, data: make(map[uint64][]byte)}
	s.pub = newDescriptorPublisherV3(d, id, other, s.descriptorIO())

	scan, err := scanWALV3(fd, d)
	if err != nil {
		return nil, err
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
		return nil, err // R < W*: fail closed (wal_corrupt)
	}
	if o.Installing {
		return nil, installingErrorV3(o)
	}
	// Replay the records <= R by LSN (last writer wins); drop the rest.
	lsnOf := make(map[uint64]uint64)
	for _, r := range scan.Records {
		e := r.Entry
		if e.LSN > o.R {
			break
		}
		s.replayV3(e, d.BlockSize, lsnOf)
	}
	// The drop fence (6.6), then the recovered head, then admission.
	floor, sameNonce := s.pub.reuseFloor()
	if err := s.fenceV3(o.E, floor, d.WALSize, sameNonce, d.WALOffset); err != nil {
		return nil, err
	}
	if _, err := s.pub.publish(&s.mu, nil, func(n *descriptorV3) error {
		n.RetainedHead = o.E
		return nil
	}); err != nil {
		return nil, err
	}
	w, err := newWALWriterV3(fd, s.pub)
	if err != nil {
		return nil, err
	}
	restore := scan
	restore.End = o.E
	if err := w.restoreScanV3(restore); err != nil {
		return nil, err
	}
	s.w = w
	s.nextLSN = o.R + 1
	s.appended, s.wack = o.R, o.R
	s.admitted = true
	return s, nil
}

func (s *walStoreV3) replayV3(e walEntryV3, blockSize uint32, lsnOf map[uint64]uint64) {
	put := func(lba, lsn uint64, b []byte) {
		if prev, ok := lsnOf[lba]; ok && prev > lsn {
			return
		}
		lsnOf[lba] = lsn
		s.data[lba] = b
	}
	switch e.Type {
	case walEntryWrite:
		put(e.LBA, e.LSN, append([]byte(nil), e.Data...))
	case walEntryWriteBatch:
		for i := uint64(0); i < e.BatchCount; i++ {
			put(e.LBA+i, e.LSN+i, append([]byte(nil), e.Data[i*uint64(blockSize):(i+1)*uint64(blockSize)]...))
		}
	case walEntryTrim:
		for i := uint64(0); i < uint64(e.Length)/uint64(blockSize); i++ {
			put(e.LBA+i, e.LSN, nil)
		}
	}
}

// fenceV3 zeroes exactly the fence ranges and fsyncs them through the leaf
// before any admission (DESIGN 6.6).
func (s *walStoreV3) fenceV3(E, floor, walSize uint64, sameNonce bool, walOffset uint64) error {
	ranges, err := fenceRangesV3(E, floor, walSize, sameNonce)
	if err != nil {
		return err
	}
	if len(ranges) == 0 || testSkipFenceOnRecoverV3 {
		return nil
	}
	if s.testBeforeFence != nil {
		s.testBeforeFence()
	}
	return s.syncFile(func() error {
		const chunk = 64 * 1024
		zero := make([]byte, chunk)
		for _, r := range ranges {
			for done := uint64(0); done < r.Len; {
				n := min(r.Len-done, chunk)
				if _, err := s.fd.WriteAt(zero[:n], int64(walOffset+r.Off+done)); err != nil {
					err = fmt.Errorf("storage: v3 drop fence: %w", err)
					s.latch(err) // inside the leaf, before fileSyncMu unlocks
					return err
				}
				done += n
			}
		}
		return nil
	}, s.fd.Sync)
}

// Write appends one single-block record. DurableLSN is the acknowledged W
// at append time. A pwrite failure latches the store-global fault.
func (s *walStoreV3) Write(lba uint32, data []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.admitted {
		return 0, errStoreNotAdmittedV3
	}
	if err := s.sticky(); err != nil {
		return 0, err
	}
	lsn := s.nextLSN
	e := walEntryV3{LSN: lsn, Type: walEntryWrite, LBA: uint64(lba), Length: uint32(len(data)), Data: data}
	if _, err := s.w.append(e, s.wack); err != nil {
		if errors.Is(err, errWALPwrite) {
			s.latch(err)
		}
		return 0, err
	}
	s.nextLSN++
	s.appended = lsn
	s.data[uint64(lba)] = append([]byte(nil), data...)
	return lsn, nil
}

// Read returns the current block (dark read path for tests).
func (s *walStoreV3) Read(lba uint32) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data[uint64(lba)]
}

// Sync acknowledges every write appended before it was called (DESIGN 6.1
// to 6.3). It returns the acknowledged W.
func (s *walStoreV3) Sync() (uint64, error) {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	s.mu.Lock()
	H, prev := s.appended, s.wack
	err := s.sticky()
	s.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if H <= prev {
		return prev, nil
	}
	switch s.pub.current().WMode {
	case WModeStrict:
		// A: fsync the WAL, then publish W = H with its own fsync.
		if err := s.syncFile(nil, s.fd.Sync); err != nil {
			return 0, err
		}
		if _, err := s.pub.publish(&s.mu, nil, func(n *descriptorV3) error { n.W = H; return nil }); err != nil {
			return 0, err
		}
	default:
		// C: the fallback slot carries the PREVIOUS batch's acknowledged W;
		// the publication's one fsync covers this batch's records and the
		// slot. The slot never names an LSN beyond a completed fsync.
		if _, err := s.pub.publish(&s.mu, nil, func(n *descriptorV3) error { n.W = prev; return nil }); err != nil {
			return 0, err
		}
	}
	s.mu.Lock()
	s.wack = max(s.wack, H)
	s.mu.Unlock()
	return H, nil
}

// refreshW is the mode-C idle refresh (DESIGN 6.2): publish W = the
// acknowledged W when the slot lags it. The flusher calls it after the
// refresh interval; tests call it directly (no timing oracle).
func (s *walStoreV3) refreshW() error {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	s.mu.Lock()
	wack := s.wack
	s.mu.Unlock()
	if wack <= s.pub.current().W {
		return nil
	}
	_, err := s.pub.publish(&s.mu, nil, func(n *descriptorV3) error { n.W = wack; return nil })
	return err
}

// SetWMode changes the W mode (DESIGN 6.4). It takes the group-commit
// leader role, so every batch admitted before it has been acknowledged
// under the old mode, and publishes the new mode with W = the actual
// acknowledged W. Every later batch uses the new mode.
func (s *walStoreV3) SetWMode(m WMode) error {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	s.mu.Lock()
	wack := s.wack
	s.mu.Unlock()
	_, err := s.pub.publish(&s.mu, nil, func(n *descriptorV3) error {
		n.WMode, n.W = m, wack
		return nil
	})
	return err
}

func (s *walStoreV3) Close() error { return s.fd.Close() }
