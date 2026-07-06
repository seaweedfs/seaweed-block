package nvme

import "fmt"

const (
	DefaultMaxH2CDataLength uint32 = 32 * 1024
	MaxH2CDataLengthLimit   uint32 = 1024 * 1024
	nvmeMinMemoryPageSize   uint32 = 4096
)

func normalizeMaxH2CDataLength(v uint32) uint32 {
	if v == 0 {
		return DefaultMaxH2CDataLength
	}
	return v
}

// ValidateMaxH2CDataLength validates the explicit NVMe/TCP H2C candidate.
// Zero means "use the product default" and is accepted.
func ValidateMaxH2CDataLength(v uint32) error {
	if v == 0 {
		return nil
	}
	if v < DefaultMaxH2CDataLength || v > MaxH2CDataLengthLimit {
		return fmt.Errorf("must be between %d and %d bytes", DefaultMaxH2CDataLength, MaxH2CDataLengthLimit)
	}
	if v%nvmeMinMemoryPageSize != 0 {
		return fmt.Errorf("must be a multiple of %d bytes", nvmeMinMemoryPageSize)
	}
	if v&(v-1) != 0 {
		return fmt.Errorf("must be a power of two")
	}
	return nil
}

func ioccszForMaxH2CDataLength(v uint32) uint32 {
	v = normalizeMaxH2CDataLength(v)
	return (64 + v) / 16
}

func mdtsForMaxH2CDataLength(v uint32) uint8 {
	v = normalizeMaxH2CDataLength(v)
	pages := (v + nvmeMinMemoryPageSize - 1) / nvmeMinMemoryPageSize
	var exp uint8
	for (uint32(1) << exp) < pages {
		exp++
	}
	return exp
}
