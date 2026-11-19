package lifecycle

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestLegacyOversizeIdenticalCreateRetry(t *testing.T) {
	dir := t.TempDir()
	spec := VolumeSpec{VolumeID: "legacy", SizeBytes: 17592186044416, ReplicationFactor: 1}
	data, err := json.Marshal(VolumeRecord{Spec: spec})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "legacy.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.CreateVolume(spec)
	if err != nil {
		t.Fatalf("existing identical intent must remain retryable: %v", err)
	}
	if got.Spec.SizeBytes != spec.SizeBytes {
		t.Fatal("existing size changed")
	}
	spec.VolumeID = "new-oversize"
	if _, err = s.CreateVolume(spec); err == nil {
		t.Fatal("new oversize admitted")
	}
}
