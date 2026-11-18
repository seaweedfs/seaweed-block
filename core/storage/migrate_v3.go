package storage

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
)

// Tool-only exports for the offline migration `sw-block migrate-v3` (format
// bump v3, DESIGN r17.4 section 10.1; plan row 13b). They reuse the row-1
// slot codec and the row-6 map codec; there is no second encoder. Nothing
// here opens, recovers or serves a store, and the v3 open code calls none of
// it.

var (
	// ErrMigrateGeometry: the v2 geometry cannot be expressed as v3 without
	// moving data (DESIGN 10.1 precondition 4).
	ErrMigrateGeometry = errors.New("storage: v2 geometry cannot be migrated in place to v3")
)

// MigrationSlotAOffset and MigrationSlotBOffset are the v3 slot positions.
const (
	MigrationSlotAOffset = slotAOffsetV3
	MigrationSlotBOffset = slotBOffsetV3
	MigrationSlotSize    = slotSizeV3
	// migratedWALMinV3 is the provisional impl-3 minimum WAL size for a
	// migrated volume: at least 64 KiB and two single-block records.
	migratedWALMinV3 = 64 * 1024
)

// MigrationSourceV3 is what the tool read from the v2 superblock.
type MigrationSourceV3 struct {
	UUID          [16]byte
	VolumeSize    uint64
	ExtentSize    uint32
	BlockSize     uint32
	OldWALOffset  uint64 // must be 4096
	OldWALSize    uint64
	CreatedAt     uint64
	CheckpointLSN uint64
	ChunkBlocks   uint64 // the map's chunk geometry (the provider's)
}

// MigrationPlanV3 is the v3 image of a migration: the descriptor slot
// images in their write order and the new descriptor's hash.
type MigrationPlanV3 struct {
	PreparedB  []byte // step 4: slot B, slotGen 1, MigrationPhase prepared
	CommittedA []byte // step 5: slot A, slotGen 2, phase 0 (the commit point)
	RolloverB  []byte // step 6: slot B, slotGen 3, phase 0
	// DescriptorHash is the SHA-256 of CommittedA, printed at step 8.
	DescriptorHash [32]byte
}

// migrationDescriptorV3 is the v3 descriptor for src (DESIGN 10.1 step 4):
// WALOffset 8192 and WALSize = old WALSize - 4096, so the extent base
// WALOffset + WALSize is unchanged; CheckpointLSN = W = the v2 checkpoint;
// an empty cursor space; BaseState none; WMode C.
func migrationDescriptorV3(src MigrationSourceV3) (descriptorV3, error) {
	if src.OldWALOffset != superblockSize {
		return descriptorV3{}, fmt.Errorf("%w: v2 WALOffset %d, want %d", ErrMigrateGeometry, src.OldWALOffset, superblockSize)
	}
	if src.BlockSize == 0 || src.OldWALSize <= slotSizeV3 {
		return descriptorV3{}, fmt.Errorf("%w: v2 WAL size %d", ErrMigrateGeometry, src.OldWALSize)
	}
	walSize := src.OldWALSize - slotSizeV3
	if walSize%uint64(src.BlockSize) != 0 || walSize < migratedWALMinV3 ||
		walSize < 2*(walEntryHeaderSizeV3+uint64(src.BlockSize)) {
		return descriptorV3{}, fmt.Errorf("%w: v3 WAL size %d is not block aligned or below the minimum", ErrMigrateGeometry, walSize)
	}
	if src.CheckpointLSN > counterMaxV3 {
		return descriptorV3{}, fmt.Errorf("%w: checkpoint %d", ErrMigrateGeometry, src.CheckpointLSN)
	}
	d := descriptorV3{
		ImplKind: ImplKindWALStore, ImplVersion: walStoreImplVersion3, UUID: src.UUID,
		VolumeSize: src.VolumeSize, ExtentSize: src.ExtentSize, BlockSize: src.BlockSize,
		WALOffset: walOffsetV3, WALSize: walSize, CreatedAt: src.CreatedAt,
		CheckpointLSN: src.CheckpointLSN, W: src.CheckpointLSN, WMode: WModeLagged,
		MapFormatID: mapFormatIDV1, MapGeneration: 1, SlotGen: 1,
	}
	if walOffsetV3+walSize != src.OldWALOffset+src.OldWALSize {
		return descriptorV3{}, fmt.Errorf("%w: the extent base would move", ErrMigrateGeometry)
	}
	if _, err := mapShapeForV3(d, src.ChunkBlocks); err != nil {
		return descriptorV3{}, fmt.Errorf("%w: map geometry: %w", ErrMigrateGeometry, err)
	}
	return d, nil
}

// ValidateMigrationV3 checks precondition 4 (geometry) without writing.
func ValidateMigrationV3(src MigrationSourceV3) error {
	_, err := migrationDescriptorV3(src)
	return err
}

// PrepareMigrationV3 runs DESIGN 10.1 step 3 and builds the slot images of
// steps 4 to 6. It (re)creates the initial map sidecar at mapPath durably:
// generation 1, every chunk UNKNOWN with maxWriteLSN 0, coveredCheckpoint =
// the v2 checkpoint (a fully flushed WAL is not remote-clean). The slots
// carry a fresh random nonce and the map fields of that root. It writes
// nothing to the store file.
func PrepareMigrationV3(mapPath string, src MigrationSourceV3) (MigrationPlanV3, error) {
	d, err := migrationDescriptorV3(src)
	if err != nil {
		return MigrationPlanV3{}, err
	}
	for _, b := range [][]byte{d.Nonce[:], d.MapLineage[:]} {
		for b2 := b; ; {
			if _, err := rand.Read(b2); err != nil {
				return MigrationPlanV3{}, err
			}
			if !allZero(b2) {
				break
			}
		}
	}
	if err := os.Remove(mapPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return MigrationPlanV3{}, err // a rerun rewrites the map (10.1 rerun table)
	}
	m, err := createChunkLSNMapV3(mapPath, d, src.ChunkBlocks)
	if err != nil {
		return MigrationPlanV3{}, err
	}
	shape, err := mapShapeForV3(d, src.ChunkBlocks)
	if err != nil {
		_ = m.Close()
		return MigrationPlanV3{}, err
	}
	entries := make([]chunkLSNEntryV3, int(shape.chunks))
	for i := range entries {
		entries[i].Unknown = true
	}
	c, err := m.Prepare(1, src.CheckpointLSN, entries, nil)
	if err = errors.Join(err, m.Close()); err != nil {
		return MigrationPlanV3{}, err
	}
	d = c.descriptor(d)
	d.SlotGen, d.MigrationPhase = 1, MigrationPrepared
	if err := d.validate(nil); err != nil {
		return MigrationPlanV3{}, err
	}
	var plan MigrationPlanV3
	plan.PreparedB = d.encode()
	d.SlotGen, d.MigrationPhase = 2, MigrationNormal
	if err := d.validate(nil); err != nil {
		return MigrationPlanV3{}, err
	}
	plan.CommittedA = d.encode()
	plan.DescriptorHash = sha256.Sum256(plan.CommittedA)
	d.SlotGen = 3
	plan.RolloverB = d.encode()
	return plan, nil
}

// RolloverSlotV3 is step 6 for a rerun that finds a committed slot A of
// slotGen 2: the same descriptor at slotGen 3, phase 0.
func RolloverSlotV3(committedA []byte) ([]byte, error) {
	d, err := decodeSlotV3(committedA, nil)
	if err != nil {
		return nil, err
	}
	if d.MigrationPhase != MigrationNormal || d.SlotGen != 2 {
		return nil, fmt.Errorf("%w: slot A is not a fresh migration commit", errSuperblockInvalid)
	}
	d.SlotGen = 3
	return d.encode(), nil
}

// MigrationSlotStateV3 classifies one raw v3 slot for the rerun table.
type MigrationSlotStateV3 int

const (
	MigrationSlotInvalid   MigrationSlotStateV3 = iota // not a valid v3 slot
	MigrationSlotPrepared                              // valid, MigrationPhase prepared
	MigrationSlotCommitted                             // valid and selectable (phase 0)
)

// ClassifyMigrationSlotV3 reports a slot's state and slotGen.
func ClassifyMigrationSlotV3(raw []byte) (MigrationSlotStateV3, uint64) {
	d, err := decodeSlotV3(raw, nil)
	if err != nil {
		return MigrationSlotInvalid, 0
	}
	if d.MigrationPhase == MigrationPrepared {
		return MigrationSlotPrepared, d.SlotGen
	}
	return MigrationSlotCommitted, d.SlotGen
}

func allZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}
