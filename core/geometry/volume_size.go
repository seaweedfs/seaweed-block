// Package geometry defines shared volume capacity limits at creation boundaries.
package geometry

import "fmt"

// MaxVolumeBytes is the uint32-LBA limit with the supported 4-KiB IO unit.
// This byte cap assumes that IO unit: support for another block size must
// enforce min(MaxVolumeBytes, (2^32-1)*blockSize) at creation as well.
const MaxVolumeBytes uint64 = (16 << 40) - 4096

// ValidateVolumeSize rejects an unsupported capacity before any allocation or
// durable create. Division also makes block geometry checks overflow-safe.
func ValidateVolumeSize(size uint64) error {
	if size > MaxVolumeBytes {
		return fmt.Errorf("volume size %d exceeds maximum %d bytes (16 TiB - 4 KiB; uint32 LBA)", size, MaxVolumeBytes)
	}
	return nil
}
