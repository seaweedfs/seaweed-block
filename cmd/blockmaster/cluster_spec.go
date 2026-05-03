package main

import (
	"fmt"
	"os"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"gopkg.in/yaml.v3"
)

type clusterSpecFile struct {
	Nodes   []clusterSpecNode   `yaml:"nodes"`
	Volumes []clusterSpecVolume `yaml:"volumes"`
}

type clusterSpecNode struct {
	ServerID string            `yaml:"server_id"`
	Addr     string            `yaml:"addr"`
	DataAddr string            `yaml:"data_addr"`
	CtrlAddr string            `yaml:"ctrl_addr"`
	Labels   map[string]string `yaml:"labels"`
	Pools    []clusterSpecPool `yaml:"pools"`
}

type clusterSpecPool struct {
	PoolID     string            `yaml:"pool_id"`
	TotalBytes uint64            `yaml:"total_bytes"`
	FreeBytes  uint64            `yaml:"free_bytes"`
	BlockSize  uint64            `yaml:"block_size"`
	Labels     map[string]string `yaml:"labels"`
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
	Nodes      []lifecycle.NodeRegistration
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
	for i, node := range spec.Nodes {
		if node.ServerID == "" {
			return clusterSpecImport{}, fmt.Errorf("cluster spec: node[%d] missing server_id", i)
		}
		reg := lifecycle.NodeRegistration{
			ServerID: node.ServerID,
			Addr:     node.Addr,
			DataAddr: node.DataAddr,
			CtrlAddr: node.CtrlAddr,
			Labels:   node.Labels,
			Pools:    make([]lifecycle.StoragePool, 0, len(node.Pools)),
		}
		for j, pool := range node.Pools {
			if pool.PoolID == "" {
				return clusterSpecImport{}, fmt.Errorf("cluster spec: node %q pool[%d] missing pool_id", node.ServerID, j)
			}
			reg.Pools = append(reg.Pools, lifecycle.StoragePool{
				PoolID:     pool.PoolID,
				TotalBytes: pool.TotalBytes,
				FreeBytes:  pool.FreeBytes,
				BlockSize:  pool.BlockSize,
				Labels:     pool.Labels,
			})
		}
		out.Nodes = append(out.Nodes, reg)
	}
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
