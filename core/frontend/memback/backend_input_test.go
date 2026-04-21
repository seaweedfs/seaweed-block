// Ownership: sw (bug regression, not in QA-locked test-spec).
// Added in response to architect review finding: write(-1, ...)
// previously panicked via negative-offset slice. Covered here so
// the negative-offset contract is pinned even if the QA spec
// doesn't enumerate it.
package memback

import (
	"errors"
	"testing"
)

func TestVolumeStore_NegativeOffset_ReturnsErrInvalidOffset(t *testing.T) {
	s := newVolumeStore()

	if _, err := s.write(-1, []byte("x")); !errors.Is(err, ErrInvalidOffset) {
		t.Fatalf("write(-1) = %v, want ErrInvalidOffset", err)
	}
	buf := make([]byte, 4)
	if _, err := s.read(-1, buf); !errors.Is(err, ErrInvalidOffset) {
		t.Fatalf("read(-1) = %v, want ErrInvalidOffset", err)
	}
	if len(s.data) != 0 {
		t.Fatalf("store mutated by bad write: len=%d", len(s.data))
	}
}
