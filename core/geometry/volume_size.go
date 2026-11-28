// Package geometry defines shared volume capacity limits at creation boundaries.
package geometry

import (
	"errors"
	"fmt"
)

// MaxVolumeBytes is the uint32-LBA limit with the supported 4-KiB IO unit.
// This byte cap assumes that IO unit: support for another block size must
// enforce min(MaxVolumeBytes, (2^32-1)*blockSize) at creation as well.
const MaxVolumeBytes uint64 = (16 << 40) - 4096

// MaxNewVolumeBytes is the largest volume this release creates (CreateVolume
// and a cluster spec's volumes). It is checked only where a volume is
// created, so a volume that already exists keeps opening under
// MaxVolumeBytes. A restore target takes its source's size.
//
// 256 GiB is 65,536 chunks of 4 MiB. A cut's commit checks the chunks it
// names within one 2 s conditional update of HEAD; the worst of five
// measured runs verified about 188,000 chunks in that time at 32 requests
// in flight. Halved for the rest of the update (the HEAD write, the
// manifest segments, a loaded or remote filer) and rounded down to a power
// of two, that is 65,536 chunks.
const MaxNewVolumeBytes uint64 = 256 << 30

// ErrVolumeTooLarge refuses a new volume above MaxNewVolumeBytes.
var ErrVolumeTooLarge = errors.New("volume too large")

// ValidateNewVolumeSize refuses a new volume above MaxNewVolumeBytes; the
// error names the maximum.
func ValidateNewVolumeSize(size uint64) error {
	if size > MaxNewVolumeBytes {
		return fmt.Errorf("%w: %d bytes exceeds the maximum of %d bytes", ErrVolumeTooLarge, size, MaxNewVolumeBytes)
	}
	return ValidateVolumeSize(size)
}

// ValidateVolumeSize rejects an unsupported capacity before any allocation or
// durable create. Division also makes block geometry checks overflow-safe.
func ValidateVolumeSize(size uint64) error {
	if size > MaxVolumeBytes {
		return fmt.Errorf("volume size %d exceeds maximum %d bytes (16 TiB - 4 KiB; uint32 LBA)", size, MaxVolumeBytes)
	}
	return nil
}

// ValidateBlockGeometry checks the original values before narrowing or I/O.
func ValidateBlockGeometry(blocks, blockSize uint64) error {
	if blocks > uint64(^uint32(0)) {
		return fmt.Errorf("block count %d exceeds uint32", blocks)
	}
	if blockSize == 0 || blockSize > uint64(^uint32(0)) {
		return fmt.Errorf("invalid block size %d", blockSize)
	}
	// Division prevents an oversized product from wrapping before admission.
	if blocks > MaxVolumeBytes/blockSize {
		return fmt.Errorf("volume geometry %d x %d exceeds maximum %d bytes (16 TiB - 4 KiB; uint32 LBA)", blocks, blockSize, MaxVolumeBytes)
	}
	return nil
}
