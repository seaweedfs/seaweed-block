package lifecycle

import (
	"errors"
	"testing"
)

func TestCreateVolumePersistsV1GeometryAndRejectsChangedReplay(t *testing.T) {
	s, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	spec := VolumeSpec{VolumeID: "v", SizeBytes: 1 << 20, ReplicationFactor: 1}
	first, err := s.CreateVolume(spec)
	if err != nil {
		t.Fatal(err)
	}
	if first.Spec.BlockSize != 4096 || first.Spec.ChunkBytes != 4<<20 {
		t.Fatalf("default geometry not persisted: %+v", first.Spec)
	}
	if first.StoreReady {
		t.Fatal("new record advertised store readiness before VOLUME and HEAD")
	}
	again, err := s.CreateVolume(spec)
	if err != nil {
		t.Fatal(err)
	}
	if again.Spec != first.Spec {
		t.Fatalf("replay changed geometry: first=%+v replay=%+v", first.Spec, again.Spec)
	}
	ready, err := s.MarkStoreReady("v", first.Spec.LayoutID)
	if err != nil || !ready.StoreReady {
		t.Fatalf("mark ready: record=%+v err=%v", ready, err)
	}
	if _, err := s.MarkStoreReady("v", "00000000000000000000000000000000"); !errors.Is(err, ErrVolumeConflict) {
		t.Fatalf("foreign layout id changed readiness: %v", err)
	}
	replayed, err := s.CreateVolume(spec)
	if err != nil || !replayed.StoreReady {
		t.Fatalf("ready replay: record=%+v err=%v", replayed, err)
	}
	changed := spec
	changed.BlockSize = 8192
	if _, err := s.CreateVolume(changed); !errors.Is(err, ErrInvalidVolumeSpec) {
		t.Fatalf("unsupported v1 geometry: %v", err)
	}
	changed.BlockSize = 4096
	changed.ChunkBytes = 1 << 20
	if _, err := s.CreateVolume(changed); !errors.Is(err, ErrInvalidVolumeSpec) {
		t.Fatalf("unsupported v1 chunk size: %v", err)
	}
}
