package launcher

import (
	"os"
	"path/filepath"
	"testing"
)

func TestG15d_ManifestWriter_WritesDeterministicFiles(t *testing.T) {
	dir := t.TempDir()
	manifests := []RenderedManifest{{
		Name: "sw-blockvolume-pvc-a-r1",
		YAML: []byte("kind: Deployment\nmetadata:\n  name: sw-blockvolume-pvc-a-r1\n"),
	}}
	if err := WriteRenderedManifests(dir, manifests); err != nil {
		t.Fatalf("WriteRenderedManifests: %v", err)
	}
	path := filepath.Join(dir, "sw-blockvolume-pvc-a-r1.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if string(raw) != string(manifests[0].YAML) {
		t.Fatalf("raw=%q want %q", raw, manifests[0].YAML)
	}
	if err := WriteRenderedManifests(dir, manifests); err != nil {
		t.Fatalf("second WriteRenderedManifests: %v", err)
	}
}

func TestG15d_ManifestWriter_RequiresDir(t *testing.T) {
	if err := WriteRenderedManifests("", nil); err == nil {
		t.Fatal("expected error")
	}
}
