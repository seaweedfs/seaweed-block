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
// Placeholder value: MaxVolumeBytes, until it is set from the measured
// capacity limit.
const MaxNewVolumeBytes uint64 = MaxVolumeBytes

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
