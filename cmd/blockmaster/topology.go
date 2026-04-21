package main

import (
	"fmt"
	"os"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"gopkg.in/yaml.v3"
)

// topologyFile is the YAML shape accepted by --topology <file>.
// Example:
//
//	volumes:
//	  - volume_id: v1
//	    slots:
//	      - replica_id: r1
//	        server_id: s1
//	      - replica_id: r2
//	        server_id: s2
//	      - replica_id: r3
//	        server_id: s3
//
// This is the operator-supplied accepted topology the
// observation host uses to classify supportability. The master
// host does NOT infer topology from heartbeats; topology is an
// explicit input, matching T0 sketch §3 constraint that
// heartbeats are observation only, never authority or policy.
type topologyFile struct {
	Volumes []topologyVolume `yaml:"volumes"`
}

type topologyVolume struct {
	VolumeID string         `yaml:"volume_id"`
	Slots    []topologySlot `yaml:"slots"`
}

type topologySlot struct {
	ReplicaID string `yaml:"replica_id"`
	ServerID  string `yaml:"server_id"`
}

// loadTopology reads the topology YAML from path and returns an
// authority.AcceptedTopology. Empty path returns empty topology;
// caller decides whether that's a hard error.
func loadTopology(path string) (authority.AcceptedTopology, error) {
	if path == "" {
		return authority.AcceptedTopology{}, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return authority.AcceptedTopology{}, fmt.Errorf("topology: read %q: %w", path, err)
	}
	var f topologyFile
	if err := yaml.Unmarshal(raw, &f); err != nil {
		return authority.AcceptedTopology{}, fmt.Errorf("topology: parse %q: %w", path, err)
	}
	return toAcceptedTopology(&f)
}

func toAcceptedTopology(f *topologyFile) (authority.AcceptedTopology, error) {
	out := authority.AcceptedTopology{}
	for i, v := range f.Volumes {
		if v.VolumeID == "" {
			return authority.AcceptedTopology{}, fmt.Errorf("topology: volume[%d] missing volume_id", i)
		}
		if len(v.Slots) == 0 {
			return authority.AcceptedTopology{}, fmt.Errorf("topology: volume %q has no slots", v.VolumeID)
		}
		slots := make([]authority.ExpectedSlot, 0, len(v.Slots))
		seenRid := make(map[string]struct{}, len(v.Slots))
		seenSid := make(map[string]struct{}, len(v.Slots))
		for j, s := range v.Slots {
			if s.ReplicaID == "" || s.ServerID == "" {
				return authority.AcceptedTopology{}, fmt.Errorf("topology: volume %q slot[%d] missing replica_id or server_id", v.VolumeID, j)
			}
			if _, dup := seenRid[s.ReplicaID]; dup {
				return authority.AcceptedTopology{}, fmt.Errorf("topology: volume %q duplicate replica_id %q", v.VolumeID, s.ReplicaID)
			}
			if _, dup := seenSid[s.ServerID]; dup {
				return authority.AcceptedTopology{}, fmt.Errorf("topology: volume %q duplicate server_id %q", v.VolumeID, s.ServerID)
			}
			seenRid[s.ReplicaID] = struct{}{}
			seenSid[s.ServerID] = struct{}{}
			slots = append(slots, authority.ExpectedSlot{
				ReplicaID: s.ReplicaID,
				ServerID:  s.ServerID,
			})
		}
		out.Volumes = append(out.Volumes, authority.VolumeExpected{
			VolumeID: v.VolumeID,
			Slots:    slots,
		})
	}
	return out, nil
}
