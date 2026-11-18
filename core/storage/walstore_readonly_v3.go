package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
)

// ErrReadOnlyNotResident refuses an offline read that needs a remote lazy
// fill. A sparse local extent is not evidence that the logical block is ZERO.
var ErrReadOnlyNotResident = errors.New("storage: readonly image needs remote materialization")

// readOnlyBlockV3 names one block of a trusted write or batch record.
type readOnlyBlockV3 struct {
	record walScannedRecordV3
	index  uint64 // block index inside a batch; 0 for a single write
}

type walStoreReaderV3 struct {
	descriptor  descriptorV3
	records     map[uint64]readOnlyBlockV3 // the WAL winner of each LBA above F
	chunkBlocks uint64
	absent      map[uint64]bool
}

// Dark row12a selector. Public OpenReadOnly still selects the legacy reader;
// activation changes that selector only after all named callers are accepted.
// No worker, recovery fence, descriptor publication or writable fd is created.
func openReadOnlyV3ForTest(path string, opts WALStoreOptions) (_ *WALStoreReader, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			_ = f.Close()
		}
	}()
	a, b := make([]byte, slotSizeV3), make([]byte, slotSizeV3)
	if _, err = f.ReadAt(a, slotAOffsetV3); err != nil {
		return nil, err
	}
	if _, err = f.ReadAt(b, slotBOffsetV3); err != nil {
		return nil, err
	}
	d, id, err := selectSlotV3(a, b, nil)
	if err != nil {
		return nil, err
	}
	otherRaw := b
	if id == slotB {
		otherRaw = a
	}
	var fallback *descriptorV3
	if od, e := decodeSlotV3(otherRaw, nil); e == nil && od.MigrationPhase == MigrationNormal {
		fallback = &od
	}
	cb, err := validateReadOnlyMapV3(path+".chunkmap", d, fallback)
	if err != nil {
		return nil, err
	}
	sb := superblock{Version: superblockVersionV3, UUID: d.UUID, VolumeSize: d.VolumeSize, ExtentSize: d.ExtentSize, BlockSize: d.BlockSize, WALOffset: d.WALOffset, WALSize: d.WALSize, WALHead: d.RetainedHead, WALTail: d.RetainedTail, WALCheckpointLSN: d.CheckpointLSN, CreatedAt: d.CreatedAt, ImplKind: d.ImplKind, ImplVersion: d.ImplVersion}
	copy(sb.Magic[:], superblockMagic)
	metadata, _, err := bindBlockMetadata(f, &sb, opts, false, true)
	if err != nil {
		return nil, err
	}
	scan, err := scanWALV3(f, d)
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
	outcome, err := decideRecoveryV3(d, recs, scan.MaxDurableLSN)
	if err != nil {
		return nil, err
	}
	if outcome.Installing {
		return nil, installingErrorV3(outcome)
	}
	v := &walStoreReaderV3{descriptor: d, records: map[uint64]readOnlyBlockV3{}, chunkBlocks: cb, absent: map[uint64]bool{}}
	for _, r := range scan.Records {
		if r.Entry.LSN > outcome.R {
			break
		}
		count := uint64(1)
		switch r.Entry.Type {
		case walEntryWrite:
		case walEntryWriteBatch:
			count = r.Entry.BatchCount // a trusted batch is <= R as a whole
		default:
			return nil, fmt.Errorf("%w: readonly v3 record type %#x", errV3NotYet, r.Entry.Type)
		}
		for i := uint64(0); i < count; i++ {
			if r.Entry.LSN+i > outcome.F {
				v.records[r.Entry.LBA+i] = readOnlyBlockV3{record: r, index: i}
			}
		}
	}
	if err = v.loadPresence(path); err != nil {
		return nil, err
	}
	keep = true
	return &WALStoreReader{fd: f, sb: &sb, metadata: metadata, v3: v}, nil
}

func validateReadOnlyMapV3(path string, d descriptorV3, fallback *descriptorV3) (uint64, error) {
	if d.MapRootLocator > math.MaxInt64-64 {
		return 0, ErrMapInvalid
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var b [8]byte
	if _, err = f.ReadAt(b[:], int64(d.MapRootLocator)+56); err != nil {
		return 0, err
	}
	cb := binary.LittleEndian.Uint64(b[:])
	if _, _, err = readChunkLSNMapV3(f, d, cb); err != nil {
		return 0, err
	}
	if fallback != nil {
		if fallback.UUID != d.UUID || fallback.MapLineage != d.MapLineage || !sameGeometryV3(*fallback, d) {
			return 0, ErrMapInvalid
		}
		if _, _, err = readChunkLSNMapV3(f, *fallback, cb); err != nil {
			return 0, err
		}
	}
	return cb, nil
}

func (v *walStoreReaderV3) loadPresence(path string) error {
	if _, err := os.Stat(path + ".chunkfresh"); err == nil {
		return ErrReadOnlyNotResident
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	data, err := os.ReadFile(path + ".chunkpresence")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		kind, num, ok := strings.Cut(scanner.Text(), " ")
		n, e := strconv.ParseUint(num, 10, 64)
		blocks := v.descriptor.VolumeSize / uint64(v.descriptor.BlockSize)
		if !ok || e != nil {
			return fmt.Errorf("%w: malformed presence", ErrReadOnlyNotResident)
		}
		switch kind {
		case "absent":
			if n >= (blocks+v.chunkBlocks-1)/v.chunkBlocks {
				return ErrReadOnlyNotResident
			}
			v.absent[n] = true
		case "local":
			if n >= blocks {
				return ErrReadOnlyNotResident
			}
		default:
			return ErrReadOnlyNotResident
		}
	}
	return scanner.Err()
}

// Caller holds the reader mutex, so Close cannot release this fd mid-read.
func (r *WALStoreReader) readV3(lba uint32) ([]byte, error) {
	v := r.v3
	d := v.descriptor
	if uint64(lba) >= d.VolumeSize/uint64(d.BlockSize) {
		return nil, fmt.Errorf("storage: readonly LBA %d out of range", lba)
	}
	if b, ok := v.records[uint64(lba)]; ok {
		record := b.record
		off := record.Entry.LogicalStart % d.WALSize
		e, _, ioErr, verr := readWholeRecordV3(r.fd, recordGeometryV3{d.WALOffset, d.WALSize, d.BlockSize, d.VolumeSize}, off)
		if ioErr != nil {
			return nil, ioErr
		}
		if verr != nil {
			return nil, NewWALIntegrityFailure(verr, "readonly v3 WAL checksum/encoding")
		}
		if e.Nonce != d.Nonce || e.LogicalStart != record.Entry.LogicalStart || e.Type != record.Entry.Type {
			return nil, NewWALIntegrityFailure(nil, "readonly v3 WAL binding changed")
		}
		data, err := blockOfRecordV3(e, uint64(lba), uint32(b.index)*d.BlockSize, record.Entry.LSN+b.index, d.BlockSize)
		if err != nil {
			return nil, NewWALIntegrityFailure(err, "readonly v3 WAL binding changed")
		}
		return data, nil
	}
	// A WAL winner is self-contained. Otherwise an absent chunk needs the
	// daemon's remote-aware read path, even if a stale local hint names a block.
	if v.absent[uint64(lba)/v.chunkBlocks] {
		return nil, ErrReadOnlyNotResident
	}
	return r.readFromExtent(lba)
}
