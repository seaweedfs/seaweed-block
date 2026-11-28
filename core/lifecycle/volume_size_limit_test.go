package lifecycle

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/geometry"
)

func TestCreateVolumeRejectsBeyondUint32LBACap(t *testing.T) {
	s, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const capBytes = geometry.MaxNewVolumeBytes // at most 16 TiB - 4 KiB (uint32 LBA)
	if _, err = s.CreateVolume(VolumeSpec{VolumeID: "at-cap", SizeBytes: capBytes, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	for _, size := range []uint64{capBytes + 1, capBytes + 4096, ^uint64(0)} {
		_, err = s.CreateVolume(VolumeSpec{VolumeID: "too-large", SizeBytes: size, ReplicationFactor: 1})
		if !errors.Is(err, ErrInvalidVolumeSpec) || !errors.Is(err, geometry.ErrVolumeTooLarge) || !strings.Contains(err.Error(), strconv.FormatUint(geometry.MaxNewVolumeBytes, 10)) {
			t.Fatalf("size %d: want clear capacity refusal, got %v", size, err)
		}
		if _, ok := s.GetVolumeSnapshot("too-large"); ok {
			t.Fatal("refusal persisted oversized volume")
		}
	}
}
