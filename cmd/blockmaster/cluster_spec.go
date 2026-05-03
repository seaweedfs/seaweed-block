package main

import (
	"fmt"
	"os"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"gopkg.in/yaml.v3"
)

type clusterSpecFile struct {
	Volumes []clusterSpecVolume `yaml:"volumes"`
}

type clusterSpecVolume struct {
	ID                string                     `yaml:"id"`
	SizeBytes         uint64                     `yaml:"size_bytes"`
	ReplicationFactor int                        `yaml:"replication_factor"`
	Placements        []clusterSpecPlacementSlot `yaml:"placements"`
}

type clusterSpecPlacementSlot struct {
	ServerID  string `yaml:"server_id"`
	ReplicaID string `yaml:"replica_id"`
	Source    string `yaml:"source"`
}

type clusterSpecImport struct {
	Topology   topologyFile
	Placements []lifecycle.PlacementIntent
}

func loadClusterSpec(path string) (clusterSpecImport, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return clusterSpecImport{}, fmt.Errorf("cluster spec: read %q: %w", path, err)
	}
	var spec clusterSpecFile
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		return clusterSpecImport{}, fmt.Errorf("cluster spec: parse %q: %w", path, err)
	}
	return clusterSpecToImports(spec)
}

func clusterSpecToImports(spec clusterSpecFile) (clusterSpecImport, error) {
	var out clusterSpecImport
	for i, vol := range spec.Volumes {
		if vol.ID == "" {
			return clusterSpecImport{}, fmt.Errorf("cluster spec: volume[%d] missing id", i)
		}
		if vol.ReplicationFactor <= 0 {
			return clusterSpecImport{}, fmt.Errorf("cluster spec: volume %q replication_factor must be > 0", vol.ID)
		}
		if len(vol.Placements) != vol.ReplicationFactor {
			return clusterSpecImport{}, fmt.Errorf("cluster spec: volume %q placements %d != replication_factor %d", vol.ID, len(vol.Placements), vol.ReplicationFactor)
		}
		topVol := topologyVolume{VolumeID: vol.ID}
		intent := lifecycle.PlacementIntent{
			VolumeID:  vol.ID,
			DesiredRF: vol.ReplicationFactor,
		}
		for j, slot := range vol.Placements {
			if slot.ServerID == "" || slot.ReplicaID == "" {
				return clusterSpecImport{}, fmt.Errorf("cluster spec: volume %q placement[%d] missing server_id or replica_id", vol.ID, j)
			}
			source := slot.Source
			if source == "" {
				source = lifecycle.PlacementSourceExistingReplica
			}
			topVol.Slots = append(topVol.Slots, topologySlot{
				ServerID:  slot.ServerID,
				ReplicaID: slot.ReplicaID,
			})
			intent.Slots = append(intent.Slots, lifecycle.PlacementSlotIntent{
				ServerID:  slot.ServerID,
				ReplicaID: slot.ReplicaID,
				Source:    source,
			})
		}
		out.Topology.Volumes = append(out.Topology.Volumes, topVol)
		out.Placements = append(out.Placements, intent)
	}
	return out, nil
}
