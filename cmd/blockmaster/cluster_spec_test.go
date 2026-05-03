package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestClusterSpec_ImportsDesiredVolumeNodeAndPlacement(t *testing.T) {
	path := writeClusterSpecForTest(t)
	imports, err := loadClusterSpec(path)
	if err != nil {
		t.Fatalf("load cluster spec: %v", err)
	}
	if len(imports.Topology.Volumes) != 1 || imports.Topology.Volumes[0].VolumeID != "v1" {
		t.Fatalf("topology=%+v want one v1 volume", imports.Topology)
	}
	if len(imports.Topology.Volumes[0].Slots) != 1 || imports.Topology.Volumes[0].Slots[0].ReplicaID != "r2" {
		t.Fatalf("topology slots=%+v want r2", imports.Topology.Volumes[0].Slots)
	}
	if len(imports.Placements) != 1 || imports.Placements[0].VolumeID != "v1" || imports.Placements[0].DesiredRF != 1 {
		t.Fatalf("placements=%+v want v1 rf=1", imports.Placements)
	}
	slot := imports.Placements[0].Slots[0]
	if slot.ServerID != "s2" || slot.ReplicaID != "r2" || slot.Source != "existing_replica" {
		t.Fatalf("placement slot=%+v want s2/r2 existing_replica", slot)
	}
}

func writeClusterSpecForTest(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "m01.yaml")
	raw := []byte(`
volumes:
  - id: v1
    size_bytes: 1048576
    replication_factor: 1
    placements:
      - server_id: s2
        replica_id: r2
        source: existing_replica
`)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write cluster spec: %v", err)
	}
	return path
}
