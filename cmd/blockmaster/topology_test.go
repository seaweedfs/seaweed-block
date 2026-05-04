package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// D5 — --topology flag integration test. Validates the YAML
// loader that feeds cmd/blockmaster. L2 subprocess tests (D1)
// consume topology via this flag, not via master.Config
// directly, so flag parsing must be proven independently.

func TestCmdBlockmaster_TopologyFlagLoaded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "topology.yaml")
	if err := os.WriteFile(path, []byte(`
volumes:
  - volume_id: v1
    slots:
      - replica_id: r1
        server_id: s1
      - replica_id: r2
        server_id: s2
      - replica_id: r3
        server_id: s3
`), 0o644); err != nil {
		t.Fatalf("write topology: %v", err)
	}

	topo, err := loadTopology(path)
	if err != nil {
		t.Fatalf("loadTopology: %v", err)
	}
	if len(topo.Volumes) != 1 {
		t.Fatalf("volumes: got %d want 1", len(topo.Volumes))
	}
	v := topo.Volumes[0]
	if v.VolumeID != "v1" {
		t.Fatalf("volume id: got %q want v1", v.VolumeID)
	}
	if len(v.Slots) != 3 {
		t.Fatalf("slots: got %d want 3", len(v.Slots))
	}
	want := []struct{ replica, server string }{
		{"r1", "s1"}, {"r2", "s2"}, {"r3", "s3"},
	}
	for i, w := range want {
		if v.Slots[i].ReplicaID != w.replica || v.Slots[i].ServerID != w.server {
			t.Fatalf("slot[%d]: got %+v want %+v", i, v.Slots[i], w)
		}
	}
}

func TestCmdBlockmaster_TopologyFlag_RejectsDuplicateReplica(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte(`
volumes:
  - volume_id: v1
    slots:
      - replica_id: r1
        server_id: s1
      - replica_id: r1
        server_id: s2
`), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := loadTopology(path)
	if err == nil {
		t.Fatal("expected error on duplicate replica_id")
	}
	if !strings.Contains(err.Error(), "duplicate replica_id") {
		t.Fatalf("error must name the violation; got: %v", err)
	}
}

func TestCmdBlockmaster_TopologyFlag_RejectsDuplicateServer(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte(`
volumes:
  - volume_id: v1
    slots:
      - replica_id: r1
        server_id: s1
      - replica_id: r2
        server_id: s1
`), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := loadTopology(path)
	if err == nil {
		t.Fatal("expected error on duplicate server_id")
	}
	if !strings.Contains(err.Error(), "duplicate server_id") {
		t.Fatalf("error must name the violation; got: %v", err)
	}
}

func TestCmdBlockmaster_TopologyFlag_EmptyIsValid(t *testing.T) {
	// Empty topology is a valid startup state (empty cluster);
	// master logs a warning but loadTopology itself succeeds.
	topo, err := loadTopology("")
	if err != nil {
		t.Fatalf("empty path: unexpected error %v", err)
	}
	if len(topo.Volumes) != 0 {
		t.Fatalf("empty path must yield zero volumes; got %d", len(topo.Volumes))
	}
}

func TestCmdBlockmaster_TopologyFlag_MissingFileRejected(t *testing.T) {
	_, err := loadTopology(filepath.Join(t.TempDir(), "nonexistent.yaml"))
	if err == nil {
		t.Fatal("expected error on missing file")
	}
}

func TestCmdBlockmaster_TopologyFlag_MalformedRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte("not: [valid yaml"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := loadTopology(path)
	if err == nil {
		t.Fatal("expected error on malformed YAML")
	}
}
