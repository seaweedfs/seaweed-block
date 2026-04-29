package recovery

// RebuildBitmap is the per-LBA "WAL lane has won base lane" flag for one
// rebuild session. It carries no internal locking — the owning
// RebuildSession serializes all access via its own mutex
// (INV-BITMAP-NO-INDEPENDENT-LOCK; V2-faithful).
//
// Semantics (INV-DUAL-LANE-WAL-WINS-BASE):
//
//	WAL lane apply       ApplyWALEntry  →  bitmap.MarkApplied(lba)
//	Base lane query      ApplyBaseBlock →  if bitmap.IsApplied(lba): skip
//
// "Mark on apply, not on receive" — the bit is set after the WAL
// entry is durably written to the substrate, so a race where base
// arrives between WAL receive and WAL apply still has base lose
// (because base's apply path takes the same session mutex and sees
// the bit already set, OR loses the lock race and writes first then
// gets overwritten by the WAL apply that follows).
type RebuildBitmap struct {
	bits []uint64 // bit per LBA; idx = lba/64, mask = 1 << (lba%64)
	n    uint32   // number of addressable LBAs
}

// NewRebuildBitmap allocates a bitmap covering [0, numBlocks).
func NewRebuildBitmap(numBlocks uint32) *RebuildBitmap {
	words := (numBlocks + 63) / 64
	return &RebuildBitmap{
		bits: make([]uint64, words),
		n:    numBlocks,
	}
}

// MarkApplied records that the WAL lane has applied this LBA.
// Subsequent ApplyBaseBlock for the same LBA must be skipped.
// Caller MUST hold the session mutex.
func (b *RebuildBitmap) MarkApplied(lba uint32) {
	if lba >= b.n {
		return
	}
	b.bits[lba/64] |= 1 << (lba % 64)
}

// IsApplied reports whether MarkApplied has been called for this LBA.
// Caller MUST hold the session mutex.
func (b *RebuildBitmap) IsApplied(lba uint32) bool {
	if lba >= b.n {
		return false
	}
	return b.bits[lba/64]&(1<<(lba%64)) != 0
}

// ShouldApplyBase is the inverted query base lane uses: returns true
// when the base block at this LBA still needs to be installed.
// Caller MUST hold the session mutex.
func (b *RebuildBitmap) ShouldApplyBase(lba uint32) bool {
	return !b.IsApplied(lba)
}

// AppliedCount returns the number of LBAs that have been marked
// (diagnostic only).
func (b *RebuildBitmap) AppliedCount() int {
	n := 0
	for _, w := range b.bits {
		// popcount; Go 1.9+ math/bits.OnesCount64 is fine but we keep
		// it inline to avoid the import for one diagnostic.
		v := w
		v = v - ((v >> 1) & 0x5555555555555555)
		v = (v & 0x3333333333333333) + ((v >> 2) & 0x3333333333333333)
		v = (v + (v >> 4)) & 0x0f0f0f0f0f0f0f0f
		n += int((v * 0x0101010101010101) >> 56)
	}
	return n
}
