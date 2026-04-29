package recovery

import "testing"

func TestRebuildBitmap_MarkAndQuery(t *testing.T) {
	b := NewRebuildBitmap(128)
	if b.IsApplied(0) {
		t.Fatal("fresh bitmap: lba 0 should not be applied")
	}
	if !b.ShouldApplyBase(0) {
		t.Fatal("fresh bitmap: ShouldApplyBase(0) want true")
	}
	b.MarkApplied(0)
	if !b.IsApplied(0) {
		t.Fatal("after MarkApplied(0): IsApplied want true")
	}
	if b.ShouldApplyBase(0) {
		t.Fatal("after MarkApplied(0): ShouldApplyBase want false")
	}
	if b.IsApplied(1) {
		t.Fatal("MarkApplied(0) should not flip bit 1")
	}
}

func TestRebuildBitmap_BoundaryLBAs(t *testing.T) {
	const n = 200
	b := NewRebuildBitmap(n)
	for _, lba := range []uint32{0, 63, 64, 127, 128, n - 1} {
		b.MarkApplied(lba)
		if !b.IsApplied(lba) {
			t.Fatalf("lba %d: IsApplied want true after mark", lba)
		}
	}
	// Out-of-range LBAs are no-ops (don't crash, don't flip).
	b.MarkApplied(n)
	b.MarkApplied(n + 1000)
	if b.IsApplied(n) {
		t.Fatal("IsApplied past end should be false")
	}
}

func TestRebuildBitmap_AppliedCount(t *testing.T) {
	b := NewRebuildBitmap(256)
	if got := b.AppliedCount(); got != 0 {
		t.Fatalf("fresh: AppliedCount=%d want 0", got)
	}
	b.MarkApplied(0)
	b.MarkApplied(63)
	b.MarkApplied(64)
	b.MarkApplied(255)
	if got := b.AppliedCount(); got != 4 {
		t.Fatalf("AppliedCount=%d want 4", got)
	}
	// Idempotent.
	b.MarkApplied(0)
	if got := b.AppliedCount(); got != 4 {
		t.Fatalf("after re-mark same lba: AppliedCount=%d want 4", got)
	}
}
