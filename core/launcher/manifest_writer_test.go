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

func TestG15e_ManifestWriter_SyncRemovesStaleRenderedFiles(t *testing.T) {
	dir := t.TempDir()
	if err := WriteRenderedManifests(dir, []RenderedManifest{
		{Name: "sw-blockvolume-old-r1", YAML: []byte("old\n")},
		{Name: "sw-blockvolume-keep-r1", YAML: []byte("old keep\n")},
	}); err != nil {
		t.Fatalf("seed manifests: %v", err)
	}
	if err := SyncRenderedManifests(dir, []RenderedManifest{
		{Name: "sw-blockvolume-keep-r1", YAML: []byte("new keep\n")},
	}); err != nil {
		t.Fatalf("SyncRenderedManifests: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "sw-blockvolume-old-r1.yaml")); !os.IsNotExist(err) {
		t.Fatalf("stale manifest still exists or stat err=%v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "sw-blockvolume-keep-r1.yaml"))
	if err != nil {
		t.Fatalf("read kept manifest: %v", err)
	}
	if string(raw) != "new keep\n" {
		t.Fatalf("kept manifest=%q", raw)
	}
}

func TestG15d_ManifestWriter_RequiresDir(t *testing.T) {
	if err := WriteRenderedManifests("", nil); err == nil {
		t.Fatal("expected error")
	}
}
