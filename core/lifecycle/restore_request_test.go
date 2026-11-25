package lifecycle

import (
	"errors"
	"testing"
)

// A restore retry keeps the original source, snapshot and request identity.
// A target name cannot be rebound to a different image after an uncertain RPC.
func TestRestoreRequestBindsTargetName(t *testing.T) {
	s, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	spec := VolumeSpec{VolumeID: "target", SizeBytes: 1 << 20, ReplicationFactor: 1,
		SourceVolume: "source", SourceSnapshot: "daily", CreateRequestID: "restore-1"}
	first, err := s.CreateVolume(spec)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.CreateVolume(spec)
	if err != nil || again.Spec.LayoutID != first.Spec.LayoutID {
		t.Fatalf("same request replay: %+v %v", again, err)
	}
	for _, mutation := range []func(*VolumeSpec){
		func(v *VolumeSpec) { v.SourceVolume = "another" },
		func(v *VolumeSpec) { v.SourceSnapshot = "weekly" },
		func(v *VolumeSpec) { v.CreateRequestID = "restore-2" },
		func(v *VolumeSpec) { v.SourceVolume, v.SourceSnapshot, v.CreateRequestID = "", "", "" },
	} {
		changed := spec
		mutation(&changed)
		if _, err := s.CreateVolume(changed); !errors.Is(err, ErrVolumeConflict) {
			t.Fatalf("rebound target %+v: %v; want conflict", changed, err)
		}
	}
}
