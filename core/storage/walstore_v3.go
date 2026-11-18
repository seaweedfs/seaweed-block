package storage

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
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
	pub        *descriptorPublisherV3
	w          *walWriterV3
	ownedMap   *chunkLSNMapV3
	mapEntries []chunkLSNEntryV3 // guarded by WALStore.mu; UNKNOWN is never cleared here
	mapProof   *mapProofEpochV3

	appended atomic.Uint64 // highest LSN pwritten to the WAL
	wack     atomic.Uint64 // highest acknowledged LSN (Sync completed)

	// recoverGen is the recovery generation. A v3 Recover holds syncMu (the
	// leader role) for its whole run, between lifecycleMu and s.mu, and bumps
	// recoverGen first, under syncMu and s.mu. Every W publisher (the Sync
	// batch, refreshW, SetWMode) holds syncMu across capture -> publish ->
	// acknowledgement, so no publication straddles a recovery (codex01 row
	// 5d; Claude01's structural seam). The one boundary that cannot hold
	// syncMu across its wait, the outer Sync() completion, captures
	// recoverGen with its target and rechecks it under syncMu -> s.mu.
	//
	// recoverGen is bumped at the start AND at the end of every recovery
	// and reset, the start+end pattern of the map proof epoch: a lock-free
	// reader whose attempt saw the same value before and after was not cut
	// by a recovery's end (codex01 2fa56078 P2: a read admitted between the
	// start bump and the dirty-map clear captured the new value with an old
	// entry, then a same-tuple rewrite after the recovery).
	//
	// Lock graph: captureMu -> lifecycleMu -> syncMu -> s.mu -> descMu ->
	// fileSyncMu; nothing takes syncMu while holding s.mu or descMu.
	recoverGen atomic.Uint64

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
	// testRefreshCached runs in refreshW after it has read the acknowledged
	// W and before it publishes; testRecoverHold runs in recoverV3 after the
	// scan and decision, before the fence; testBatchBeforeAck runs in a
	// committer batch after its publication and before its final
	// acknowledgement; testSyncBeforeCompletion runs in the outer Sync()
	// after the committer returned and before its completion.
	testRefreshCached        func()
	testRecoverHold          func()
	testBatchBeforeAck       func()
	testSyncBeforeCompletion func()
	testMapPrepared          func() // outside all publication locks, after owned-map persistence
	// testReadResolved runs in a v3 read after the dirty map resolved the
	// LBA's WAL position and before the record is read.
	testReadResolved func()
	// testReadInvalid runs in a v3 read after a record failed validation and
	// before the entry is re-resolved.
	testReadInvalid func()
	// testReadAdmitted runs in a v3 read before it captures the recovery
	// generation; testRecoverAfterBump runs in recoverV3 after the start
	// bump and before the dirty map is cleared.
	testReadAdmitted     func()
	testRecoverAfterBump func()

	// installMu is the install admission lock (DESIGN 8.7), the topmost lock
	// of section 4: begin and complete hold it exclusive, a handle mutation
	// shared. liveInstallID is the ID this open began (written under
	// installMu and s.mu, so either guards a read); an installing ID from
	// before the open is a physical reopen's (plan row 11).
	installMu     sync.RWMutex
	liveInstallID [16]byte
	// installEpoch counts the begins of this open: mutators hold installMu
	// and s.mu; a catch-up scan reads it under s.mu to detect BASE ABA. A
	// handle is valid only while no later begin has run.
	installEpoch uint64
	// installReopen is the last Recover's installing outcome (guarded by
	// installMu and s.mu), consumed by a covering superseding begin.
	installReopen *installReopenV3
	// ownerAttachDurable: this open's chunk attach was durably committed
	// by the provider (CommitChunkAttachV3); false at open.
	ownerAttachDurable atomic.Bool
	// preBaseHighest is the highest appended LSN when the live install
	// began (guarded by installMu): the records before its BaseCursor.
	preBaseHighest uint64
	// testInstallBeforeAdmit runs in a handle mutation before it acquires
	// installMu; testInstallAdmitted after its ID check, before the write.
	testInstallBeforeAdmit func()
	testInstallAdmitted    func()
	// testInstallLogBeforeAck runs in SyncInstallLog after its publication
	// and before its final ack-boundary check.
	testInstallLogBeforeAck func()
	// testScanRead runs in a v3 ScanLBAs after its scan and before its
	// revalidation.
	testScanRead func()
}

// errV3RecoveredMeanwhile: a recovery ran between an outer Sync's capture
// of its target and its completion; nothing is acknowledged.
var errV3RecoveredMeanwhile = errors.New("storage: v3 recovery ran meanwhile; the Sync is not acknowledged")

// errV3LegacySuperblockWrite: a legacy superblock write (offset 0, which is
// descriptor slot A on a v3 file) was attempted on a v3 store.
var errV3LegacySuperblockWrite = errors.New("storage: legacy superblock write refused on a v3 store (offset 0 is descriptor slot A)")

var errV3NotYet = errors.New("storage: operation not yet supported on a v3 store (later plan row)")

// ErrUnsupportedLegacyFormat requires offline migration or workspace restore;
// a production open never interprets a v2 volume as v3.
var ErrUnsupportedLegacyFormat = errors.New("storage: unsupported legacy walstore format; migrate offline or restore into a new volume")

func refuseLegacyWALStore(f *os.File) error {
	if sb, err := readSuperblock(io.NewSectionReader(f, 0, superblockSize)); err == nil && sb.Version == 2 && sb.validate() == nil {
		return ErrUnsupportedLegacyFormat
	}
	return nil
}

func metadataSlotAcceptorV3(opts WALStoreOptions) slotImplAcceptor {
	return func(kind ImplKind, version uint32) bool {
		return ossSlotImplAcceptor(kind, version) ||
			(kind == ImplKindWALStore && version >= walStoreImplVersion3 && opts.MetadataFactory != nil)
	}
}

// ProbeWALStoreV3 identifies a valid OSS descriptor through the same A/B
// selector as open. It grants no ownership or serving permission; Open
// acquires the file lock and validates all map dependencies afresh.
func ProbeWALStoreV3(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	a, b := make([]byte, slotSizeV3), make([]byte, slotSizeV3)
	if _, err := f.ReadAt(a, 0); err != nil {
		return err
	}
	if _, err := f.ReadAt(b, slotBOffsetV3); err != nil {
		return err
	}
	_, _, err = selectSlotV3(a, b, nil)
	return err
}

// testSkipFenceOnRecoverV3 is a test-only mutant switch for the G1 control
// (it proves the schedule resurrects without the fence). Never set outside
// tests.
var testSkipFenceOnRecoverV3 bool

// FormatV3 reports whether the store runs the dark v3 format (only test
// constructors build one before activation, plan row 12b).
func (s *WALStore) FormatV3() bool { return s.v3 != nil }

func (s *WALStore) latchV3(err error) {
	if err != nil {
		s.ioFault.CompareAndSwap(nil, NewSubstrateIOFailure(err,
			"v3 durable persistence failed; the open store refuses further Syncs, writes and checkpoints"))
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
	return createWALStoreV3MapAtCursorForTest(path, numBlocks, blockSize, walSize, mode, cursor, 0)
}

// Explicit geometry selector for dark row7 caller controls. Zero is reserved
// for earlier unmapped row5 fixtures; production activation must never use it.
func createMappedWALStoreV3ForTest(path string, numBlocks, blockSize uint32, walSize uint64, mode WMode, chunkBlocks uint64) (*WALStore, error) {
	if chunkBlocks == 0 {
		return nil, ErrMapInvalid
	}
	return createWALStoreV3MapAtCursorForTest(path, numBlocks, blockSize, walSize, mode, 0, chunkBlocks)
}
func createWALStoreV3MapAtCursorForTest(path string, numBlocks, blockSize uint32, walSize uint64, mode WMode, cursor, chunkBlocks uint64) (*WALStore, error) {
	return createWALStoreV3Configured(path, numBlocks, blockSize, walSize, mode, cursor, chunkBlocks, WALStoreOptions{})
}

func createWALStoreV3Configured(path string, numBlocks, blockSize uint32, walSize uint64, mode WMode, cursor, chunkBlocks uint64, opts WALStoreOptions) (*WALStore, error) {
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
	if err := LockWALStoreFile(fd); err != nil {
		_ = fd.Close()
		return nil, err
	}
	sb := superblock{Version: superblockVersionV3, ImplVersion: d.ImplVersion, BlockSize: d.BlockSize, VolumeSize: d.VolumeSize, WALOffset: d.WALOffset, WALSize: d.WALSize}
	metadata, totalSize, err := bindBlockMetadata(fd, &sb, opts, true, false)
	if err != nil {
		_ = fd.Close()
		return nil, err
	}
	d.ImplVersion = sb.ImplVersion
	if err := d.validate(metadataSlotAcceptorV3(opts)); err != nil {
		_ = fd.Close()
		return nil, err
	}
	err = fd.Truncate(totalSize)
	if err == nil && chunkBlocks != 0 {
		var m *chunkLSNMapV3
		m, err = createChunkLSNMapV3(path+".chunkmap", d, chunkBlocks)
		if err == nil {
			shape, e := mapShapeForV3(d, chunkBlocks)
			if e != nil {
				err = e
			} else if shape.chunks > uint64(int(^uint(0)>>1))/16 {
				err = ErrMapInvalid
			} else {
				entries := make([]chunkLSNEntryV3, int(shape.chunks))
				for i := range entries {
					entries[i].Unknown = true
				}
				var c *chunkMapCandidateV3
				c, err = m.Prepare(1, 0, entries, nil)
				if err == nil {
					d = c.descriptor(d)
				}
			}
			err = errors.Join(err, m.Close())
		}
	}
	if err == nil {
		_, err = fd.WriteAt(d.encode(), slotAOffsetV3)
	}
	if err == nil {
		err = fd.Sync()
	}
	if err != nil {
		_ = fd.Close()
		return nil, err
	}
	s, err := openWALStoreV3File(path, fd, chunkBlocks != 0, opts, metadata, true)
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
	return openWALStoreV3MapInternalForTest(path, true)
}

// Only pre-row7 dark fixtures can opt out of requiring a map. Even these
// fixtures must validate a non-placeholder root; production has no opt-out.
func openUnmappedWALStoreV3ForTest(path string) (*WALStore, error) {
	return openWALStoreV3MapInternalForTest(path, false)
}
func openWALStoreV3MapInternalForTest(path string, requireMap bool) (*WALStore, error) {
	return openWALStoreV3WithOptions(path, requireMap, WALStoreOptions{})
}

func openWALStoreV3WithOptions(path string, requireMap bool, opts WALStoreOptions) (*WALStore, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", path, err)
	}
	if err := LockWALStoreFile(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	return openWALStoreV3File(path, f, requireMap, opts, nil, false)
}

// f is already locked. Creation retains this same descriptor and metadata
// binding throughout initialization, so there is no close/reopen owner gap.
func openWALStoreV3File(path string, f *os.File, requireMap bool, opts WALStoreOptions, metadata BlockMetadata, creating bool) (*WALStore, error) {
	if err := refuseLegacyWALStore(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	accept := metadataSlotAcceptorV3(opts)
	a := make([]byte, slotSizeV3)
	b := make([]byte, slotSizeV3)
	_, errA := f.ReadAt(a, slotAOffsetV3)
	_, errB := f.ReadAt(b, slotBOffsetV3)
	if err := errors.Join(errA, errB); err != nil {
		_ = f.Close()
		return nil, err
	}
	d, id, err := selectSlotV3(a, b, accept)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	var other *descriptorV3
	otherRaw := b
	if id == slotB {
		otherRaw = a
	}
	if od, oerr := decodeSlotV3(otherRaw, accept); oerr == nil && od.MigrationPhase == MigrationNormal {
		other = &od
	}
	var ownedMap *chunkLSNMapV3
	var mapCurrent *chunkMapCandidateV3
	// Earlier dark row5 fixtures have no map tuple. A real immutable root is
	// never optional: missing metadata is not a clean map.
	if requireMap || d.MapRootHash != [32]byte{} {
		ownedMap, mapCurrent, err = openDescriptorMapV3(path+".chunkmap", d, other)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
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
	if !creating && opts.MetadataFactory != nil {
		metadata, _, err = bindBlockMetadata(f, &sb, opts, false, false)
		if err != nil {
			if ownedMap != nil {
				_ = ownedMap.Close()
			}
			_ = f.Close()
			return nil, err
		}
	}
	s, err := openInitializedInternal(path, f, &sb, metadata, func(s *WALStore) error {
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
		v.pub.accept = accept
		// A reset stopped between its two publications (DESIGN 8.6) binds no
		// writer across the two nonces: Recover's head publication completes
		// the new incarnation in the other slot and creates the writer then;
		// until then every append is refused.
		if !v.pub.halfResetV3() {
			w, err := newWALWriterV3(f, v.pub)
			if err != nil {
				return err
			}
			v.w = w
			v.w.instr = &s.instr
		}
		v.wack.Store(d.W)
		if ownedMap != nil {
			v.mapProof = newMapProofEpochV3()
			if err := v.pub.bindMapV3(mapCurrent, v.mapProof); err != nil {
				return err
			}
			v.ownedMap = ownedMap
			v.mapEntries = append([]chunkLSNEntryV3(nil), mapCurrent.entries...)
			ownedMap.latchPersistence = s.latchV3
		}
		s.v3 = v
		return nil
	})
	if err != nil {
		if ownedMap != nil {
			_ = ownedMap.Close()
		}
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

// recoverV3 is Recover for a v3 store (DESIGN 6.5, 6.6). The caller holds
// captureMu, lifecycleMu and s.mu, as v2 Recover does.
func (s *WALStore) recoverV3() (uint64, error) {
	v := s.v3
	v.recoverGen.Add(1)       // under syncMu and s.mu (see recoverGen); odd now
	defer v.recoverGen.Add(1) // even again at the end, on every path
	if h := v.testRecoverAfterBump; h != nil {
		h()
	}
	d := v.pub.current()
	s.dm.clear()
	scan, err := scanWALV3Accepted(s.fd, d, v.pub.accept)
	if err != nil {
		if d.BaseState == BaseInstalling {
			// Installing keeps its typed not-ready outcome; the cause is a
			// substrate error, not proven install-log damage (codex01 5b P2).
			return 0, fmt.Errorf("%w: scan: %w", ErrBaseIncomplete, err)
		}
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
		// The recovered bounds of an installing reopen (DESIGN 8.3), kept for
		// a covering supersession: E, R and whether the suffix is damaged.
		// Nothing is fenced, published or admitted here; the allocator
		// state continues above every trusted record so a later install
		// never reuses an LSN the WAL still holds.
		//
		// Only a physical reopen records them: when this open began the
		// installing ID, its handle stays valid (DESIGN r17.4: until the next
		// begin) and may append and acknowledge past this cut, so a cached E
		// would be stale and fencing from it could erase acknowledged
		// records (codex01 row 11b P1). A resume or a fenced supersession
		// then refuses before any I/O; the live install continues.
		if d.BaseInstallID != v.liveInstallID {
			ro := &installReopenV3{id: d.BaseInstallID, E: o.E, R: o.R, damaged: o.Damaged, walSize: d.WALSize}
			for _, r := range scan.Records {
				if r.Entry.LSN > o.R {
					break
				}
				if r.Entry.Type == walEntryWrite || r.Entry.Type == walEntryWriteBatch {
					ro.records = append(ro.records, r)
				}
			}
			v.installReopen = ro
			if v.w != nil {
				kept := scan
				kept.End, kept.Frontier = o.E, o.R
				if err := v.w.rememberReclaimScanV3(kept); err != nil {
					return 0, err
				}
			}
		} else if o.Damaged || v.w == nil || o.E != v.w.logicalHeadV3() {
			// A same-open Recover of this open's live install that finds the
			// WAL cut short of the live writer (damage under a live install):
			// the handle cannot safely continue past it, so the store fails
			// closed; a physical reopen then decides intact or damaged.
			s.latchV3(fmt.Errorf("storage: v3 same-open installing recovery: recovered cut E %d is not the live writer head", o.E))
		} else {
			// The live install continues (r17.4). Its trusted install-lane
			// records are republished in the dirty map, which this Recover
			// cleared, so the completed image serves them and not older BASE
			// bytes; the volatile map coverage (the BASE writes' UNKNOWN) is
			// kept as it is (codex01 04180c33 P1).
			for _, r := range scan.Records {
				if r.Entry.LSN > o.R {
					break
				}
				s.replayRecordV3(r, d.WALSize, d.BlockSize)
			}
		}
		v.appended.Store(max(v.appended.Load(), o.R))
		s.nextLSN = max(s.nextLSN, o.R+1)
		s.walHead = max(s.walHead, o.R)
		return 0, installingErrorV3(o)
	}
	// Reconstruct volatile map coverage from the authoritative immutable root,
	// then replay only the retained prefix. A same-open Recover must not keep
	// speculative cache changes from a dropped suffix.
	if v.ownedMap != nil {
		v.pub.descMu.Lock()
		v.mapEntries = append(v.mapEntries[:0], v.pub.mapCurrent.entries...)
		v.pub.descMu.Unlock()
	}
	// Replay the records <= R into the dirty map (last writer wins); the
	// rest are dropped and fenced below.
	for _, r := range scan.Records {
		e := r.Entry
		if e.LSN > o.R {
			break
		}
		if !s.replayRecordV3(r, d.WALSize, d.BlockSize) {
			s.dm.clear()
			return 0, fmt.Errorf("%w: replay of v3 record type %#x", errV3NotYet, e.Type)
		}
		// Records at/below the selected checkpoint are already represented by
		// its durable map. Re-resolving those same logical bytes is not a new
		// lower/equal mutation; conservatively account only the retained suffix.
		count := uint64(1)
		if e.Type == walEntryWriteBatch {
			count = e.BatchCount
		}
		for i := uint64(0); i < count; i++ {
			if e.LSN+i > d.CheckpointLSN {
				s.recordMapWriteV3(e.LBA+i, e.LSN+i)
			}
		}
	}
	if h := v.testRecoverHold; h != nil {
		h()
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
	if v.w == nil {
		// A half-published reset, resolved by the head publication above.
		w, err := newWALWriterV3(s.fd, v.pub)
		if err != nil {
			s.dm.clear()
			return 0, err
		}
		v.w = w
		v.w.instr = &s.instr
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
	// The volatile acknowledged frontier is the recovered prefix, never an
	// older in-memory maximum: a same-open recovery that dropped mode C's
	// newest batch must not let a later refresh publish a W the WAL no
	// longer holds (codex01 5b P1).
	v.appended.Store(o.R)
	v.wack.Store(o.R)
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
	if s.v3.w == nil {
		return 0, errWALBindingV3 // a half-published reset before its Recover
	}
	// Reserve proof invalidation before the WAL can change. This is under
	// the existing store lock; a refused append only causes conservative retry.
	if s.v3.mapProof != nil {
		if err := s.v3.mapProof.mutatedV3(); err != nil {
			return 0, err
		}
	}
	phys, err := s.v3.w.append(e, s.v3.wack.Load())
	if err != nil {
		if errors.Is(err, errWALPwrite) {
			s.latchV3(err)
		}
		return 0, err
	}
	s.recordMapWriteV3(uint64(lba), lsn)
	s.v3.appended.Store(lsn)
	return phys, nil
}

// writeBatchV3Locked is WriteBatch on a v3 store, under WALStore.mu: one
// multi-block record (type 0x04) of len(blocks) consecutive LBAs with the
// LSNs firstLSN.. firstLSN+n-1, so a crash keeps the whole batch or none of
// it (one CRC32C); the writer consumes the whole LSN range (DESIGN 6.7,
// r16). A single block is an ordinary write record. Each block's dirty
// entry names its data offset inside the record.
func (s *WALStore) writeBatchV3Locked(startLBA uint32, blocks [][]byte, firstLSN uint64) ([]uint64, error) {
	start := time.Now()
	if len(blocks) == 1 {
		phys, err := s.appendWriteV3(startLBA, blocks[0], firstLSN)
		s.instr.appendLatency.Observe(time.Since(start))
		s.walMutation.Add(1)
		if err != nil {
			return nil, fmt.Errorf("storage: WAL append: %w", err)
		}
		dirtyStart := time.Now()
		s.dm.put(uint64(startLBA), phys, firstLSN, uint32(len(blocks[0])))
		s.instr.recordDirtyMapUpdate(1, time.Since(dirtyStart))
		s.noteChunkMutation(uint64(startLBA))
		s.advanceAfterAppendV3(firstLSN, firstLSN)
		return []uint64{firstLSN}, nil
	}
	if s.v3.w == nil {
		return nil, errWALBindingV3 // a half-published reset before its Recover
	}
	bs := int(s.sb.BlockSize)
	payload := make([]byte, 0, len(blocks)*bs)
	copyStart := time.Now()
	for _, b := range blocks {
		payload = append(payload, b...)
	}
	s.instr.recordWALCopy(len(payload), time.Since(copyStart))
	n := uint64(len(blocks))
	lastLSN := firstLSN + n - 1
	if s.v3.mapProof != nil {
		if err := s.v3.mapProof.mutatedV3(); err != nil {
			return nil, err
		}
	}
	e := walEntryV3{LSN: firstLSN, BatchCount: n, Type: walEntryWriteBatch, LBA: uint64(startLBA), Length: uint32(len(payload)), Data: payload}
	phys, err := s.v3.w.append(e, s.v3.wack.Load())
	s.instr.appendLatency.Observe(time.Since(start))
	s.walMutation.Add(1)
	if err != nil {
		if errors.Is(err, errWALPwrite) {
			s.latchV3(err)
		}
		return nil, fmt.Errorf("storage: WAL batch append: %w", err)
	}
	lsns := make([]uint64, n)
	dirtyStart := time.Now()
	for i := uint64(0); i < n; i++ {
		lba := uint64(startLBA) + i
		lsns[i] = firstLSN + i
		s.recordMapWriteV3(lba, lsns[i])
		s.dm.putAt(lba, phys, uint32(i)*uint32(bs), lsns[i], uint32(bs))
		s.noteChunkMutation(lba)
	}
	s.instr.recordDirtyMapUpdate(len(blocks), time.Since(dirtyStart))
	s.v3.appended.Store(lastLSN)
	s.advanceAfterAppendV3(firstLSN, lastLSN)
	return lsns, nil
}

// advanceAfterAppendV3 moves the volatile frontier past an appended range.
// Caller holds WALStore.mu.
func (s *WALStore) advanceAfterAppendV3(first, last uint64) {
	s.nextLSN = last + 1
	if last > s.walHead {
		s.walHead = last
	}
	if s.walTail == 0 {
		s.walTail = first
	}
}

// replayRecordV3 republishes one trusted write or batch record in the dirty
// map (last writer wins per LBA). It reports false for other types.
func (s *WALStore) replayRecordV3(r walScannedRecordV3, walSize uint64, blockSize uint32) bool {
	e := r.Entry
	count := uint64(1)
	switch e.Type {
	case walEntryWrite:
	case walEntryWriteBatch:
		count = e.BatchCount
	default:
		return false
	}
	size := uint64(walEntryHeaderSizeV3 + len(e.Data))
	phys := (r.End - size) % walSize
	for i := uint64(0); i < count; i++ {
		s.dm.putAtIfNewer(e.LBA+i, phys, uint32(i)*blockSize, e.LSN+i, blockSize)
	}
	return true
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
	// One sample per covering WAL batch, as on v2. In mode C the slot
	// publication is the physical sync; mode A also settles its W slot.
	syncStart := time.Now()
	defer func() { s.fileSyncLatency.Observe(time.Since(syncStart)) }()
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
	if h := v.testBatchBeforeAck; h != nil {
		h()
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
	if h := v.testRefreshCached; h != nil {
		h()
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
	_, err := v.pub.publish(nil, nil, func(n *descriptorV3) error { n.WMode, n.W = m, wack; return nil })
	return err
}

// readDirtyV3 serves a v3 read from the dirty map, if the LBA has an entry
// (handled false: read the extent). A reader holds no lock across its WAL
// read, so the position it resolved may be reused before the pread: after a
// reset (cursor 0 again, codex01 row 8 P1) or a same-open recovery that
// fenced and reused it. The read therefore validates the WHOLE record it
// read, payload included (CRC, footer, write type, LBA and LSN against the
// resolving entry), and re-resolves on a mismatch. LSN allocation runs on
// across a reset, so a matching LBA and LSN identify the resolved write;
// no nonce is checked. A same-open recovery can re-allocate a dropped LSN
// to the same LBA at the same position, so the "entry unchanged" damage
// verdict also requires the same recovery generation before and after the
// whole attempt. A recovery bumps it at its start and its end, so any
// overlap changes it (codex01 e72acd40 and 2fa56078 P2s; see recoverGen).
// A generation captured while a recovery runs can match only if the
// verdict also falls inside that recovery, which holds s.mu: no write can
// re-allocate the tuple, and the replay installs only validated records. A pread error is a
// substrate I/O failure, never WAL integrity: only a record that fails
// validation within one stable lifetime is damage. No lock is taken: the
// lock graph is unchanged.
func (s *WALStore) readDirtyV3(lba uint32) (data []byte, handled bool, err error) {
	const attempts = 8
	for i := 0; i < attempts; i++ {
		if h := s.v3.testReadAdmitted; h != nil {
			h()
		}
		gen := s.v3.recoverGen.Load()
		off, dataOff, lsn, _, ok := s.dm.get(uint64(lba))
		if !ok {
			return nil, false, nil
		}
		if h := s.v3.testReadResolved; h != nil {
			h()
		}
		data, ioErr, verr := s.readRecordV3(lba, off, dataOff, lsn)
		if ioErr != nil {
			return nil, true, NewSubstrateIOFailure(ioErr, "v3 WAL read")
		}
		if verr == nil {
			return data, true, nil
		}
		if h := s.v3.testReadInvalid; h != nil {
			h()
		}
		off2, dataOff2, lsn2, _, ok2 := s.dm.get(uint64(lba))
		if ok2 && off2 == off && dataOff2 == dataOff && lsn2 == lsn && s.v3.recoverGen.Load() == gen {
			return nil, true, NewWALIntegrityFailure(verr, "v3 WAL read of a live dirty entry")
		}
	}
	return nil, true, fmt.Errorf("storage: v3 WAL read of LBA %d did not settle in %d attempts", lba, attempts)
}

// readRecordV3 reads and validates the whole v3 write record at walRelOff.
// ioErr is a failed or short pread: the WAL region lies inside the file, so
// io.EOF (a file shorter than its geometry) is substrate I/O too. verr is a
// record that was read but does not validate as the resolved write.
func (s *WALStore) readRecordV3(lba uint32, walRelOff uint64, dataOff uint32, lsn uint64) (data []byte, ioErr, verr error) {
	e, _, ioErr, verr := readWholeRecordV3(s.fd, recordGeometryV3{s.sb.WALOffset, s.sb.WALSize, s.sb.BlockSize, s.sb.VolumeSize}, walRelOff)
	if ioErr != nil || verr != nil {
		return nil, ioErr, verr
	}
	data, err := blockOfRecordV3(e, uint64(lba), dataOff, lsn, s.sb.BlockSize)
	if err != nil {
		return nil, nil, fmt.Errorf("storage: v3 WAL read at %d: %w", walRelOff, err)
	}
	return data, nil, nil
}

// readWholeRecordV3 reads the prefix at walRelOff, sizes the record from
// its type and Length (a batch carries BatchCount blocks), reads and
// decodes the whole record (CRC32C, footer, geometry). An append never lets
// a record cross the ring end (padding fills the suffix), so one crossing
// it is invalid. A pread error is substrate I/O (ioErr), never integrity.
// recordGeometryV3 is what a whole-record read needs of the format.
type recordGeometryV3 struct {
	WALOffset, WALSize uint64
	BlockSize          uint32
	VolumeSize         uint64
}

func readWholeRecordV3(fd *os.File, sb recordGeometryV3, walRelOff uint64) (e walEntryV3, size uint64, ioErr, verr error) {
	if walRelOff+walEntryHeaderSizeV3 > sb.WALSize {
		return e, 0, nil, fmt.Errorf("storage: v3 WAL record at %d crosses the ring end", walRelOff)
	}
	prefix := make([]byte, walEntryPrefixSizeV3)
	if _, err := fd.ReadAt(prefix, int64(sb.WALOffset+walRelOff)); err != nil {
		return e, 0, fmt.Errorf("storage: v3 WAL read at %d: %w", walRelOff, err), nil
	}
	size = uint64(walEntryHeaderSizeV3)
	switch prefix[16] {
	case walEntryWrite, walEntryWriteBatch:
		size += uint64(binary.LittleEndian.Uint32(prefix[26:30]))
	default:
		return e, 0, nil, fmt.Errorf("storage: v3 WAL record at %d has type %#x, not a write", walRelOff, prefix[16])
	}
	if walRelOff+size > sb.WALSize {
		return e, 0, nil, fmt.Errorf("storage: v3 WAL record at %d crosses the ring end", walRelOff)
	}
	full := make([]byte, size)
	if _, err := fd.ReadAt(full, int64(sb.WALOffset+walRelOff)); err != nil {
		return e, 0, fmt.Errorf("storage: v3 WAL read at %d: %w", walRelOff, err), nil
	}
	e, err := decodeWALEntryV3(full, sb.BlockSize, sb.VolumeSize/uint64(sb.BlockSize))
	if err != nil {
		return e, 0, nil, err
	}
	return e, size, nil, nil
}

// blockOfRecordV3 returns the block of a validated write or batch record
// that the dirty entry (lba, dataOff, lsn) names: a single write is block 0
// at the record's LBA and LSN; a batch's block i is at LBA+i with LSN+i and
// data offset i * blockSize.
func blockOfRecordV3(e walEntryV3, lba uint64, dataOff uint32, lsn uint64, blockSize uint32) ([]byte, error) {
	if dataOff%blockSize != 0 {
		return nil, fmt.Errorf("data offset %d not block aligned", dataOff)
	}
	idx := uint64(dataOff / blockSize)
	count := uint64(1)
	switch e.Type {
	case walEntryWrite:
	case walEntryWriteBatch:
		count = e.BatchCount
	default:
		return nil, fmt.Errorf("record type %#x is not a write", e.Type)
	}
	if idx >= count || e.LBA+idx != lba || e.LSN+idx != lsn || uint64(len(e.Data)) != count*uint64(blockSize) {
		return nil, fmt.Errorf("record is LBA %d LSN %d count %d, want block %d at LBA %d LSN %d", e.LBA, e.LSN, count, idx, lba, lsn)
	}
	off := idx * uint64(blockSize)
	return e.Data[off : off+uint64(blockSize)], nil
}

// readDirtyRecordV3 is the flusher's record read for a v3 store: it decodes
// and validates the whole record before its data may reach the extent.
func (f *flusher) readDirtyRecordV3(e snapshotEntry) ([]byte, uint64, error) {
	store := f.store
	d := store.v3.pub.current()
	entry, size, ioErr, verr := readWholeRecordV3(store.fd, recordGeometryV3{store.sb.WALOffset, store.sb.WALSize, store.sb.BlockSize, store.sb.VolumeSize}, e.WALOffset)
	if ioErr != nil {
		return nil, 0, fmt.Errorf("flusher: read v3 WAL record at %d: %w", e.WALOffset, ioErr)
	}
	if verr != nil {
		return nil, 0, fmt.Errorf("flusher: invalid v3 WAL record LBA %d offset %d: %w", e.LBA, e.WALOffset, verr)
	}
	if entry.Nonce != d.Nonce || e.Length != store.sb.BlockSize {
		return nil, 0, fmt.Errorf("flusher: v3 WAL slot mismatch LBA %d offset %d", e.LBA, e.WALOffset)
	}
	data, err := blockOfRecordV3(entry, e.LBA, e.DataOffset, e.LSN, store.sb.BlockSize)
	if err != nil {
		return nil, 0, fmt.Errorf("flusher: v3 WAL slot mismatch LBA %d offset %d: %w", e.LBA, e.WALOffset, err)
	}
	return data, size, nil
}

// persistCheckpointV3 is the v3 checkpoint/tail publication. It goes
// through the row-3 publisher with the row-7 map candidate; without one,
// the checkpoint and tail do not move (no coverage is fabricated) and the
// flushed bytes stay retained in the WAL.
//
// A v3 checkpoint never exceeds W (DESIGN 2.2 item 5, 9.1 predicate 2:
// CheckpointLSN <= mapCoveredCheckpoint <= W). The flusher may have flushed
// dirty entries that no Sync has acknowledged yet, so the candidate is
// clamped to the acknowledged W; those records stay retained in the WAL
// until a Sync covers them. The map candidate is prepared first, outside
// every lock; the publication is a W publisher, so it runs under the v3
// leader role (syncMu), like Sync, refreshW and SetWMode, and cannot
// straddle a recovery. If a recovery lowered the acknowledged W below the
// prepared candidate meanwhile, this cycle publishes nothing.
// flushLifetimeValidV3 is checked under syncMu at publication and store.mu
// at retirement. Recover/reset advance the same start/end generation.
func (v *walStoreV3State) flushLifetimeValidV3(gen uint64) bool {
	return gen%2 == 0 && v.recoverGen.Load() == gen
}

func (s *WALStore) persistCheckpointV3(highestLSN, recoveryGen uint64) error {
	v := s.v3
	if v.ownedMap != nil {
		return s.persistOwnedMapCheckpointV3(highestLSN, recoveryGen)
	}
	if v.mapCandidate == nil {
		return nil
	}
	checkpoint := min(highestLSN, v.wack.Load())
	if checkpoint <= v.pub.current().CheckpointLSN {
		return nil
	}
	mutateMap, err := v.mapCandidate(checkpoint)
	if err != nil {
		return err
	}
	v.syncMu.Lock()
	defer v.syncMu.Unlock()
	if !v.flushLifetimeValidV3(recoveryGen) {
		return ErrMapCoverageStale
	}
	wack := v.wack.Load()
	if wack < checkpoint {
		return nil // a recovery lowered the frontier; retry next cycle
	}
	d, err := v.pub.publish(&s.mu, nil, func(n *descriptorV3) error {
		n.W = max(n.W, wack)
		n.CheckpointLSN = checkpoint
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
