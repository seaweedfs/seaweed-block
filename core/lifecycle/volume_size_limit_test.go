package lifecycle

import (
	"errors"
	"strings"
	"testing"
)

func TestCreateVolumeRejectsBeyondUint32LBACap(t *testing.T) {
	s, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const capBytes = uint64(17592186040320) // 16 TiB - 4 KiB
	if _, err = s.CreateVolume(VolumeSpec{VolumeID: "at-cap", SizeBytes: capBytes, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	for _, size := range []uint64{capBytes + 1, capBytes + 4096, ^uint64(0)} {
		_, err = s.CreateVolume(VolumeSpec{VolumeID: "too-large", SizeBytes: size, ReplicationFactor: 1})
		if !errors.Is(err, ErrInvalidVolumeSpec) || !strings.Contains(err.Error(), "16 TiB - 4 KiB") {
			t.Fatalf("size %d: want clear capacity refusal, got %v", size, err)
		}
		if _, ok := s.GetVolumeSnapshot("too-large"); ok {
			t.Fatal("refusal persisted oversized volume")
		}
	}
}
