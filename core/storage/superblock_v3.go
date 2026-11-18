package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
)

// Impl-3 descriptor slots (format bump v3, DESIGN r15 section 2).
//
// A v3 file holds two 4096-byte descriptor slots, A at offset 0 and B at
// 4096; the WAL starts at 8192. Each slot is one complete generation
// descriptor sealed by a CRC32C (Castagnoli). This file holds only the
// slot codec, its validity rules and the production A/B selection. Legacy
// decoding is retained for the explicit offline migration boundary.

const (
	slotSizeV3           = 4096
	slotAOffsetV3        = 0
	slotBOffsetV3        = slotSizeV3
	walOffsetV3          = 2 * slotSizeV3
	superblockVersionV3  = 3
	walStoreImplVersion3 = 3
	mapFormatIDV1        = 1

	// counterMaxV3 is the largest value any persisted counter may hold
	// (DESIGN 2.4): an operation that would exceed it is refused, so
	// nothing ever wraps.
	counterMaxV3 = math.MaxUint64 - 1

	slotCRCOffsetV3 = slotSizeV3 - 4
)

// BaseState is the persisted BASE-install state (DESIGN 2.1, 8.5).
type BaseState uint8

const (
	BaseNone       BaseState = 0
	BaseInstalling BaseState = 1
	BaseInstalled  BaseState = 2
)

// WMode selects the synced-watermark protocol (DESIGN 6.1).
type WMode uint8

const (
	WModeLagged WMode = 0 // C, the default: one fsync, lagged slot W
	WModeStrict WMode = 1 // A, opt-in: two fsyncs
)

// MigrationPhase marks a slot written by the offline v2 to v3 migration
// before its commit (DESIGN 10.1). A prepared slot is never selectable.
type MigrationPhase uint8

const (
	MigrationNormal   MigrationPhase = 0
	MigrationPrepared MigrationPhase = 1
)

var (
	// ErrMigrationIncomplete: no selectable slot exists but a prepared
	// migration slot does (DESIGN 2.3, 10.1). The volume must not serve.
	ErrMigrationIncomplete = errors.New("storage: v3 migration not committed (only a prepared descriptor slot exists)")
	// ErrCounterExhausted: a persisted counter would exceed 2^64-2
	// (DESIGN 2.4). Nothing is written.
	ErrCounterExhausted = errors.New("storage: v3 counter exhausted")
)

// descriptorV3 is one decoded v3 descriptor slot. Field order and offsets
// follow DESIGN 2.1 exactly; see encode/decodeSlotV3.
type descriptorV3 struct {
	ImplKind    ImplKind
	ImplVersion uint32
	UUID        [16]byte
	VolumeSize  uint64
	ExtentSize  uint32
	BlockSize   uint32
	WALOffset   uint64
	WALSize     uint64
	CreatedAt   uint64
	SlotGen     uint64

	CheckpointLSN uint64

	Nonce                [16]byte
	RetainedTail         uint64
	RetainedHead         uint64
	MapFormatID          uint32
	MapGeneration        uint64
	MapRootLocator       uint64
	MapCoveredCheckpoint uint64
	MapRootHash          [32]byte
	MapLineage           [16]byte

	W              uint64
	BaseLSN        uint64
	BaseState      BaseState
	BaseCursor     uint64
	BaseInstallID  [16]byte
	WMode          WMode
	MigrationPhase MigrationPhase
}

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// reservedRangesV3 are the [start, end) byte ranges that must be zero.
var reservedRangesV3 = [][2]int{
	{9, 12}, {88, 128}, {164, 168}, {240, 256}, {273, 280}, {306, slotCRCOffsetV3},
}

// encode serializes d into one sealed slot. It does not validate; callers
// validate before publishing (publishDescriptor) and tests use it to build
// vectors.
func (d *descriptorV3) encode() []byte {
	b := make([]byte, slotSizeV3)
	le := binary.LittleEndian
	copy(b[0:4], superblockMagic)
	le.PutUint16(b[4:], superblockVersionV3)
	le.PutUint16(b[6:], 0) // flags
	b[8] = uint8(d.ImplKind)
	le.PutUint32(b[12:], d.ImplVersion)
	copy(b[16:32], d.UUID[:])
	le.PutUint64(b[32:], d.VolumeSize)
	le.PutUint32(b[40:], d.ExtentSize)
	le.PutUint32(b[44:], d.BlockSize)
	le.PutUint64(b[48:], d.WALOffset)
	le.PutUint64(b[56:], d.WALSize)
	le.PutUint64(b[64:], d.CreatedAt)
	le.PutUint64(b[72:], d.SlotGen)
	le.PutUint64(b[80:], d.CheckpointLSN)
	copy(b[128:144], d.Nonce[:])
	le.PutUint64(b[144:], d.RetainedTail)
	le.PutUint64(b[152:], d.RetainedHead)
	le.PutUint32(b[160:], d.MapFormatID)
	le.PutUint64(b[168:], d.MapGeneration)
	le.PutUint64(b[176:], d.MapRootLocator)
	le.PutUint64(b[184:], d.MapCoveredCheckpoint)
	copy(b[192:224], d.MapRootHash[:])
	copy(b[224:240], d.MapLineage[:])
	le.PutUint64(b[256:], d.W)
	le.PutUint64(b[264:], d.BaseLSN)
	b[272] = uint8(d.BaseState)
	le.PutUint64(b[280:], d.BaseCursor)
	copy(b[288:304], d.BaseInstallID[:])
	b[304] = uint8(d.WMode)
	b[305] = uint8(d.MigrationPhase)
	le.PutUint32(b[slotCRCOffsetV3:], crc32.Checksum(b[:slotCRCOffsetV3], crc32cTable))
	return b
}

// slotImplAcceptor reports whether an (ImplKind, ImplVersion) pair is
// accepted in a v3 slot. OSS accepts walstore impl 3 only; impl 4 is
// reserved for an accepting enterprise metadata factory (DESIGN 1).
type slotImplAcceptor func(kind ImplKind, version uint32) bool

func ossSlotImplAcceptor(kind ImplKind, version uint32) bool {
	return kind == ImplKindWALStore && version == walStoreImplVersion3
}

// decodeSlotV3 decodes and validates one slot (DESIGN 2.2 items 1-3 and 5,
// and the slot-local part of the 9.1 coverage predicates). Map dependency
// validation (item 4: root and pages present, in bounds, hash-valid) needs
// the sidecar and is applied by the map layer on top of this.
func decodeSlotV3(b []byte, accept slotImplAcceptor) (descriptorV3, error) {
	if len(b) != slotSizeV3 {
		return descriptorV3{}, fmt.Errorf("%w: slot length %d", errSuperblockInvalid, len(b))
	}
	le := binary.LittleEndian
	if string(b[0:4]) != superblockMagic {
		return descriptorV3{}, errSuperblockNotOurs
	}
	if v := le.Uint16(b[4:]); v != superblockVersionV3 {
		return descriptorV3{}, fmt.Errorf("%w: slot version %d, want %d", errSuperblockBadVer, v, superblockVersionV3)
	}
	if got, want := le.Uint32(b[slotCRCOffsetV3:]), crc32.Checksum(b[:slotCRCOffsetV3], crc32cTable); got != want {
		return descriptorV3{}, fmt.Errorf("%w: slot CRC32C %08x, want %08x", errSuperblockInvalid, got, want)
	}
	if f := le.Uint16(b[6:]); f != 0 {
		return descriptorV3{}, fmt.Errorf("%w: slot flags %#x", errSuperblockInvalid, f)
	}
	for _, r := range reservedRangesV3 {
		if i := firstNonZero(b[r[0]:r[1]]); i >= 0 {
			return descriptorV3{}, fmt.Errorf("%w: reserved byte %d is nonzero", errSuperblockInvalid, r[0]+i)
		}
	}
	var d descriptorV3
	d.ImplKind = ImplKind(b[8])
	d.ImplVersion = le.Uint32(b[12:])
	copy(d.UUID[:], b[16:32])
	d.VolumeSize = le.Uint64(b[32:])
	d.ExtentSize = le.Uint32(b[40:])
	d.BlockSize = le.Uint32(b[44:])
	d.WALOffset = le.Uint64(b[48:])
	d.WALSize = le.Uint64(b[56:])
	d.CreatedAt = le.Uint64(b[64:])
	d.SlotGen = le.Uint64(b[72:])
	d.CheckpointLSN = le.Uint64(b[80:])
	copy(d.Nonce[:], b[128:144])
	d.RetainedTail = le.Uint64(b[144:])
	d.RetainedHead = le.Uint64(b[152:])
	d.MapFormatID = le.Uint32(b[160:])
	d.MapGeneration = le.Uint64(b[168:])
	d.MapRootLocator = le.Uint64(b[176:])
	d.MapCoveredCheckpoint = le.Uint64(b[184:])
	copy(d.MapRootHash[:], b[192:224])
	copy(d.MapLineage[:], b[224:240])
	d.W = le.Uint64(b[256:])
	d.BaseLSN = le.Uint64(b[264:])
	d.BaseState = BaseState(b[272])
	d.BaseCursor = le.Uint64(b[280:])
	copy(d.BaseInstallID[:], b[288:304])
	d.WMode = WMode(b[304])
	d.MigrationPhase = MigrationPhase(b[305])
	if err := d.validate(accept); err != nil {
		return descriptorV3{}, err
	}
	return d, nil
}

func firstNonZero(b []byte) int {
	for i, c := range b {
		if c != 0 {
			return i
		}
	}
	return -1
}

func invalidV3(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{errSuperblockInvalid}, args...)...)
}

// validate applies the field rules of DESIGN 2.1/2.2 that need no sidecar.
func (d *descriptorV3) validate(accept slotImplAcceptor) error {
	if accept == nil {
		accept = ossSlotImplAcceptor
	}
	if !accept(d.ImplKind, d.ImplVersion) {
		return invalidV3("impl kind %d version %d not accepted in a v3 slot", d.ImplKind, d.ImplVersion)
	}
	// Identity and counters (2.2 item 2, 2.4).
	if d.SlotGen == 0 || d.SlotGen > counterMaxV3 {
		return invalidV3("slotGen %d", d.SlotGen)
	}
	if d.Nonce == ([16]byte{}) {
		return invalidV3("all-zero WAL nonce")
	}
	for _, c := range []struct {
		name string
		v    uint64
	}{
		{"CheckpointLSN", d.CheckpointLSN}, {"retainedHead", d.RetainedHead},
		{"mapGeneration", d.MapGeneration}, {"W", d.W}, {"BaseLSN", d.BaseLSN},
	} {
		if c.v > counterMaxV3 {
			return invalidV3("%s %d exceeds 2^64-2", c.name, c.v)
		}
	}
	// Geometry and bounds (2.2 item 3), with checked arithmetic.
	if d.WALOffset != walOffsetV3 {
		return invalidV3("WALOffset %d, want %d", d.WALOffset, walOffsetV3)
	}
	if d.BlockSize == 0 || d.ExtentSize == 0 || d.WALSize == 0 || d.VolumeSize == 0 {
		return invalidV3("zero geometry (block %d extent %d wal %d volume %d)",
			d.BlockSize, d.ExtentSize, d.WALSize, d.VolumeSize)
	}
	if d.VolumeSize%uint64(d.BlockSize) != 0 {
		return invalidV3("VolumeSize %d not aligned to BlockSize %d", d.VolumeSize, d.BlockSize)
	}
	if end, ok := checkedAddV3(d.WALOffset, d.WALSize); !ok {
		return invalidV3("WAL region overflows")
	} else if _, ok := checkedAddV3(end, d.VolumeSize); !ok {
		return invalidV3("extent region overflows")
	}
	if d.RetainedTail > d.RetainedHead {
		return invalidV3("retainedTail %d > retainedHead %d", d.RetainedTail, d.RetainedHead)
	}
	if d.RetainedHead-d.RetainedTail > d.WALSize {
		return invalidV3("retained window %d exceeds WALSize %d", d.RetainedHead-d.RetainedTail, d.WALSize)
	}
	// Map fields named by the slot (2.1; 9.1 predicate 2, slot-local part).
	if d.MapFormatID != mapFormatIDV1 {
		return invalidV3("mapFormatID %d", d.MapFormatID)
	}
	if d.MapRootLocator%slotSizeV3 != 0 {
		return invalidV3("map root locator %d not 4096-aligned", d.MapRootLocator)
	}
	if d.MapCoveredCheckpoint < d.CheckpointLSN {
		return invalidV3("map covers %d, below CheckpointLSN %d", d.MapCoveredCheckpoint, d.CheckpointLSN)
	}
	if d.MapCoveredCheckpoint > d.W {
		return invalidV3("map covers %d, above W %d", d.MapCoveredCheckpoint, d.W)
	}
	// BASE fields (2.1 BASE rules).
	var zeroID [16]byte
	switch d.BaseState {
	case BaseNone:
		if d.BaseLSN != 0 || d.BaseCursor != 0 || d.BaseInstallID != zeroID {
			return invalidV3("BaseState none with BASE fields set")
		}
	case BaseInstalling:
		if d.BaseInstallID == zeroID {
			return invalidV3("installing with a zero BaseInstallID")
		}
		if d.BaseCursor < d.RetainedTail || d.BaseCursor > d.RetainedHead {
			return invalidV3("installing BaseCursor %d outside [%d, %d]", d.BaseCursor, d.RetainedTail, d.RetainedHead)
		}
	case BaseInstalled:
		if d.BaseInstallID == zeroID {
			return invalidV3("installed with a zero BaseInstallID")
		}
		// BaseCursor may lie below retainedTail after retirement (r15).
		if d.BaseCursor > d.RetainedHead {
			return invalidV3("installed BaseCursor %d above retainedHead %d", d.BaseCursor, d.RetainedHead)
		}
	default:
		return invalidV3("BaseState %d", d.BaseState)
	}
	if d.WMode != WModeLagged && d.WMode != WModeStrict {
		return invalidV3("WMode %d", d.WMode)
	}
	switch d.MigrationPhase {
	case MigrationNormal:
	case MigrationPrepared:
		if d.SlotGen != 1 || d.BaseState != BaseNone {
			return invalidV3("prepared slot with slotGen %d BaseState %d", d.SlotGen, d.BaseState)
		}
	default:
		return invalidV3("MigrationPhase %d", d.MigrationPhase)
	}
	return nil
}

// checkedAddV3 adds without wrapping and refuses results above 2^64-2.
func checkedAddV3(a, b uint64) (uint64, bool) {
	s := a + b
	if s < a || s > counterMaxV3 {
		return 0, false
	}
	return s, true
}

// nextSlotGenV3 is the slotGen for the next publication (DESIGN 2.4).
func nextSlotGenV3(cur uint64) (uint64, error) {
	n, ok := checkedAddV3(cur, 1)
	if !ok {
		return 0, fmt.Errorf("%w: slotGen %d", ErrCounterExhausted, cur)
	}
	return n, nil
}

// slotIDV3 names a descriptor slot.
type slotIDV3 int

const (
	slotA slotIDV3 = 0
	slotB slotIDV3 = 1
)

// selectSlotV3 applies DESIGN 2.3 to the raw bytes of slots A and B and
// returns the selected descriptor and which slot it came from.
func selectSlotV3(a, b []byte, accept slotImplAcceptor) (descriptorV3, slotIDV3, error) {
	da, errA := decodeSlotV3(a, accept)
	db, errB := decodeSlotV3(b, accept)
	if errA != nil && errB != nil {
		return descriptorV3{}, 0, fmt.Errorf("%w: neither descriptor slot is valid (A: %v; B: %v)",
			errSuperblockInvalid, errA, errB)
	}
	if errA == nil && errB == nil {
		if da.UUID != db.UUID || !sameGeometryV3(da, db) {
			return descriptorV3{}, 0, invalidV3("slots A and B disagree on identity or geometry")
		}
		if da.SlotGen == db.SlotGen && !bytes.Equal(a, b) {
			return descriptorV3{}, 0, invalidV3("slots A and B share slotGen %d with different bytes", da.SlotGen)
		}
	}
	selA := errA == nil && da.MigrationPhase == MigrationNormal
	selB := errB == nil && db.MigrationPhase == MigrationNormal
	switch {
	case selA && selB:
		if db.SlotGen > da.SlotGen {
			return db, slotB, nil
		}
		return da, slotA, nil
	case selA:
		return da, slotA, nil
	case selB:
		return db, slotB, nil
	default:
		return descriptorV3{}, 0, ErrMigrationIncomplete
	}
}

func sameGeometryV3(x, y descriptorV3) bool {
	return x.VolumeSize == y.VolumeSize && x.ExtentSize == y.ExtentSize &&
		x.BlockSize == y.BlockSize && x.WALOffset == y.WALOffset && x.WALSize == y.WALSize
}
